import { useEffect, useMemo, useRef, useState } from "react"
import cytoscape from "cytoscape"
import type { GraphResponse, RecipeIOFluid, RecipeIOItem } from "./types"
import { fetchGraph, fetchMachinesByOutput, fetchRecipesByOutput, searchFluids, searchItems } from "./api"

const SEARCH_DEBOUNCE_MS = 300
const OUTPUT_SEARCH_LIMIT = 30
const TIERS = ["ULV", "LV", "MV", "HV", "EV", "IV", "LuV", "ZPM", "UV", "UHV"]
const RECIPE_RESULTS_LIMIT = 200

type Target = {
  type: "item" | "fluid"
  id: string
  meta: number
  name?: string
}

type MachineOption = {
  machine_id: string
  machine_name: string
}

type RecipeOption = {
  rid: string
  machine_id: string
  machine_name?: string
  duration_ticks: number
  eut: number
  min_tier?: string
  min_voltage?: number
  amps?: number
  item_inputs?: RecipeIOItem[]
  fluid_inputs?: RecipeIOFluid[]
  item_outputs?: RecipeIOItem[]
  fluid_outputs?: RecipeIOFluid[]
}

export default function App() {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const cyRef = useRef<cytoscape.Core | null>(null)

  const [outputTarget, setOutputTarget] = useState<Target | null>(null)
  const [selectedRecipe, setSelectedRecipe] = useState<RecipeOption | null>(null)
  const [ratePerMin, setRatePerMin] = useState(1)
  const [overclock, setOverclock] = useState(0)
  const [parallel, setParallel] = useState(1)
  const [recipeTierOverrides, setRecipeTierOverrides] = useState<Record<string, string>>({})
  const [recipeOverclockTiers, setRecipeOverclockTiers] = useState<Record<string, number>>({})

  const [graph, setGraph] = useState<GraphResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [graphTab, setGraphTab] = useState<"graph" | "machines">("graph")
  const [configTab, setConfigTab] = useState<"outputs" | "inputs" | "options">("outputs")

  const [showOutputModal, setShowOutputModal] = useState(true)
  const [outputQuery, setOutputQuery] = useState("")
  const [outputResults, setOutputResults] = useState<Target[]>([])
  const [selectedOutput, setSelectedOutput] = useState<Target | null>(null)
  const [outputRecipes, setOutputRecipes] = useState<RecipeOption[]>([])
  const [selectedMachineId, setSelectedMachineId] = useState<string | null>(null)
  const [isLoadingOutputs, setIsLoadingOutputs] = useState(false)
  const [isLoadingMachines, setIsLoadingMachines] = useState(false)
  const [isLoadingRecipes, setIsLoadingRecipes] = useState(false)
  const [machinesForOutput, setMachinesForOutput] = useState<MachineOption[]>([])

  const outputKey = outputTarget
    ? outputTarget.type === "item"
      ? `item:${outputTarget.id}:${outputTarget.meta}`
      : `fluid:${outputTarget.id}`
    : null

  useEffect(() => {
    if (!containerRef.current) return
    if (cyRef.current) return

    cyRef.current = cytoscape({
      container: containerRef.current,
      style: [
        {
          selector: "node",
          style: {
            "background-color": "#8d6a46",
            "label": "data(label)",
            "color": "#f4efe6",
            "font-family": "Space Grotesk, sans-serif",
            "font-weight": "600",
            "text-valign": "center",
            "text-halign": "center",
            "text-wrap": "wrap",
            "text-max-width": "140px"
          }
        },
        {
          selector: "node[type = \"recipe\"]",
          style: {
            "background-color": "#2e4c5b",
            "shape": "round-rectangle"
          }
        },
        {
          selector: "node[type = \"fluid\"]",
          style: {
            "background-color": "#476b6b"
          }
        },
        {
          selector: "edge",
          style: {
            "line-color": "#d1b38c",
            "target-arrow-color": "#d1b38c",
            "target-arrow-shape": "triangle",
            "curve-style": "bezier",
            "label": "data(label)",
            "font-family": "Space Grotesk, sans-serif",
            "font-size": "10px",
            "color": "#f4efe6",
            "text-background-color": "#1c1a18",
            "text-background-opacity": 0.6,
            "text-background-padding": "2px",
            "line-style": "solid"
          }
        },
        {
          selector: "edge[kind = \"consumes\"][material_type = \"item\"]",
          style: {
            "line-color": "#6aa1d6",
            "target-arrow-color": "#6aa1d6"
          }
        },
        {
          selector: "edge[kind = \"consumes\"][material_type = \"fluid\"]",
          style: {
            "line-color": "#6fd0b8",
            "target-arrow-color": "#6fd0b8"
          }
        },
        {
          selector: "edge[kind = \"produces\"]",
          style: {
            "line-color": "#d1b38c",
            "target-arrow-color": "#d1b38c"
          }
        },
        {
          selector: "edge[kind = \"byproduct\"]",
          style: {
            "line-color": "#8aa0a3",
            "target-arrow-color": "#8aa0a3"
          }
        },
        {
          selector: "edge[material_type = \"fluid\"]",
          style: {
            "line-style": "dashed"
          }
        }
      ],
      layout: {
        name: "breadthfirst",
        directed: true,
        padding: 30,
        spacingFactor: 1.2
      },
      wheelSensitivity: 0.15,
      minZoom: 0.2,
      maxZoom: 2
    })
  }, [])

  const formatMachines = (value?: number) => {
    if (value === undefined || Number.isNaN(value)) return "?"
    return Number.isInteger(value) ? String(value) : value.toFixed(1)
  }

  const tierIndex = (tier?: string) => (tier ? TIERS.indexOf(tier) : -1)
  const getOverclockTiers = (minTier?: string, selectedTier?: string) => {
    const minIndex = tierIndex(minTier)
    const selectedIndex = tierIndex(selectedTier)
    if (minIndex < 0 || selectedIndex < 0) return 0
    return Math.max(0, selectedIndex - minIndex)
  }
  const getTierOptions = (minTier?: string) => {
    const minIndex = tierIndex(minTier)
    return minIndex >= 0 ? TIERS.slice(minIndex) : TIERS
  }
  const getTierForRid = (rid?: string) => {
    if (!rid) return "?"
    return recipeTierOverrides[rid] || (selectedRecipe?.rid === rid ? selectedRecipe.min_tier : "?") || "?"
  }

  useEffect(() => {
    if (!outputTarget || !selectedRecipe) return
    runGraph()
  }, [outputTarget, selectedRecipe])

  useEffect(() => {
    if (!cyRef.current || !graph) return
    const cy = cyRef.current
    cy.elements().remove()

    const nodeMap = new Map(graph.nodes.map(node => [node.id, node]))
    const formatFluidRate = (ratePerS: number) => {
      const liters = ratePerS / 1000
      return `${liters.toFixed(liters >= 10 ? 1 : 2)} L/s`
    }
    const formatItemRate = (ratePerS: number) => `${ratePerS.toFixed(2)} /s`
    const formatRecipeLabel = (node: any) => {
      const name = node.machine_name || node.machine_id || node.label
      const machines = formatMachines(node.machines_required)
      const tier = getTierForRid(node.rid)
      return `${name} x ${machines} (${tier})`
    }

    const elements = [
      ...graph.nodes.map(node => ({
        data: {
          ...node,
          label: node.type === "recipe" ? formatRecipeLabel(node) : node.label
        }
      })),
      ...graph.edges.map(edge => ({
        data: {
          ...edge,
          material_type: (() => {
            const source = nodeMap.get(edge.source)
            const target = nodeMap.get(edge.target)
            return source?.type === "fluid" || target?.type === "fluid" ? "fluid" : "item"
          })(),
          label: (() => {
            const source = nodeMap.get(edge.source)
            const target = nodeMap.get(edge.target)
            const isFluid = source?.type === "fluid" || target?.type === "fluid"
            return isFluid ? formatFluidRate(edge.rate_per_s) : formatItemRate(edge.rate_per_s)
          })()
        }
      }))
    ]
    cy.add(elements)
    const incoming = new Set(graph.edges.map(edge => edge.target))
    const rootIds = graph.nodes.filter(node => !incoming.has(node.id)).map(node => node.id)
    const roots = rootIds.length
      ? cy.collection(rootIds.map(id => cy.getElementById(id)))
      : undefined
    const layout = cy.layout({
      name: "breadthfirst",
      directed: true,
      padding: 30,
      spacingFactor: 1.2,
      roots
    })
    layout.run()
    cy.nodes().forEach(node => {
      const pos = node.position()
      node.position({ x: pos.y, y: pos.x })
    })
    cy.fit(undefined, 30)
  }, [graph, recipeTierOverrides, selectedRecipe])

  const recipeStats = useMemo(() => {
    if (!graph) return []
    return graph.nodes
      .filter(node => node.type === "recipe")
      .map(node => ({
        id: node.id,
        rid: node.rid,
        label: node.label,
        machines: node.machines_required,
        duration_ticks: node.duration_ticks,
        eut: node.eut,
        overclock_tiers: node.overclock_tiers
      }))
  }, [graph])

  const machineGroups = useMemo(() => {
    const groups = new Map<string, { machine_id: string; machine_name: string; recipes: RecipeOption[] }>()
    for (const recipe of outputRecipes) {
      const machineId = recipe.machine_id
      const machineName = recipe.machine_name || recipe.machine_id
      if (!groups.has(machineId)) {
        groups.set(machineId, { machine_id: machineId, machine_name: machineName, recipes: [] })
      }
      groups.get(machineId)?.recipes.push(recipe)
    }
    return Array.from(groups.values()).sort((a, b) =>
      a.machine_name.localeCompare(b.machine_name)
    )
  }, [outputRecipes])

  useEffect(() => {
    if (outputTarget) return
    setSelectedOutput(null)
    setSelectedMachineId(null)
    setOutputRecipes([])
    setMachinesForOutput([])
    setShowOutputModal(true)
  }, [outputTarget])

  useEffect(() => {
    if (!showOutputModal) return
    const trimmed = outputQuery.trim()
    if (trimmed.length < 2) {
      setOutputResults([])
      return
    }
    const handle = setTimeout(() => {
      setIsLoadingOutputs(true)
      Promise.all([
        searchItems(trimmed, OUTPUT_SEARCH_LIMIT),
        searchFluids(trimmed, OUTPUT_SEARCH_LIMIT)
      ])
        .then(([itemData, fluidData]) => {
          const items = (itemData.items || []).map((item: any) => ({
            type: "item" as const,
            id: item.item_id,
            meta: item.meta,
            name: item.name || item.item_id
          }))
          const fluids = (fluidData.fluids || []).map((fluid: any) => ({
            type: "fluid" as const,
            id: fluid.fluid_id,
            meta: 0,
            name: fluid.name || fluid.fluid_id
          }))
          setOutputResults([...items, ...fluids])
        })
        .catch(() => setOutputResults([]))
        .finally(() => setIsLoadingOutputs(false))
    }, SEARCH_DEBOUNCE_MS)
    return () => clearTimeout(handle)
  }, [showOutputModal, outputQuery])

  useEffect(() => {
    if (!selectedOutput) {
      setOutputRecipes([])
      setMachinesForOutput([])
      return
    }
    setSelectedMachineId(null)
    setOutputRecipes([])
    setIsLoadingMachines(true)
    fetchMachinesByOutput({
      output_type: selectedOutput.type,
      item_id: selectedOutput.type === "item" ? selectedOutput.id : undefined,
      meta: selectedOutput.type === "item" ? selectedOutput.meta : undefined,
      fluid_id: selectedOutput.type === "fluid" ? selectedOutput.id : undefined,
      limit: 200
    })
      .then(data => setMachinesForOutput(data.machines || []))
      .catch(() => setMachinesForOutput([]))
      .finally(() => setIsLoadingMachines(false))
  }, [selectedOutput])

  useEffect(() => {
    if (!selectedOutput || !selectedMachineId) {
      setOutputRecipes([])
      return
    }
    setIsLoadingRecipes(true)
    fetchRecipesByOutput({
      output_type: selectedOutput.type,
      item_id: selectedOutput.type === "item" ? selectedOutput.id : undefined,
      meta: selectedOutput.type === "item" ? selectedOutput.meta : undefined,
      fluid_id: selectedOutput.type === "fluid" ? selectedOutput.id : undefined,
      machine_id: selectedMachineId,
      limit: RECIPE_RESULTS_LIMIT
    })
      .then(data => setOutputRecipes(data.recipes || []))
      .catch(() => setOutputRecipes([]))
      .finally(() => setIsLoadingRecipes(false))
  }, [selectedOutput, selectedMachineId])

  const applyOutputSelection = (nextTarget: Target, recipe: RecipeOption) => {
    const selectedTier = recipeTierOverrides[recipe.rid] || recipe.min_tier
    setOutputTarget(nextTarget)
    setSelectedRecipe(recipe)
    setRecipeTierOverrides(
      selectedTier ? { [recipe.rid]: selectedTier } : {}
    )
    setRecipeOverclockTiers(
      selectedTier
        ? { [recipe.rid]: getOverclockTiers(recipe.min_tier, selectedTier) }
        : {}
    )
    setGraph(null)
    setError(null)
    setShowOutputModal(false)
    setConfigTab("outputs")
    setGraphTab("graph")
  }

  const openOutputModal = () => {
    setShowOutputModal(true)
    setSelectedOutput(null)
    setSelectedMachineId(null)
    setOutputRecipes([])
    setMachinesForOutput([])
    setOutputQuery("")
    setOutputResults([])
  }

  async function runGraph() {
    setError(null)
    if (!outputTarget || !selectedRecipe || !outputKey) {
      setError("Select an output before building the graph")
      return
    }
    try {
      const payload = {
        targets: [
          {
            target_type: outputTarget.type,
            target_id: outputTarget.id,
            target_meta: outputTarget.meta,
            target_rate_per_s: ratePerMin / 60
          }
        ],
        max_depth: 0,
        overclock_tiers: overclock,
        parallel: parallel,
        recipe_override: { [outputKey]: selectedRecipe.rid },
        recipe_overclock_tiers: recipeOverclockTiers
      }
      const data = await fetchGraph(payload)
      setGraph(data)
      setGraphTab("graph")
    } catch (err) {
      setError("Failed to build graph")
    }
  }

  return (
    <div className="app">
      <header className="hero">
        <div>
          <p className="eyebrow">GT New Horizons Planner</p>
          <h1>Production Graph Builder</h1>
          <p className="subtitle">Calculate machine counts per recipe chain and visualize GTNH throughput.</p>
        </div>
        <div className="stat-card">
          <div>
            <span className="stat-label">Rate (each)</span>
            <span className="stat-value">{ratePerMin.toFixed(2)}/min</span>
          </div>
          <div>
            <span className="stat-label">Overclock tiers</span>
            <span className="stat-value">{overclock}</span>
          </div>
          <div>
            <span className="stat-label">Parallel</span>
            <span className="stat-value">{parallel}</span>
          </div>
        </div>
      </header>

      <section className="main-layout">
        <div className="graph-panel">
          <div className="panel-tabs">
            <button
              className={graphTab === "graph" ? "active" : ""}
              onClick={() => setGraphTab("graph")}
            >
              Graph
            </button>
            <button
              className={graphTab === "machines" ? "active" : ""}
              onClick={() => setGraphTab("machines")}
            >
              Machines
            </button>
          </div>
          <div className="panel-body">
            <div className={`graph-section ${graphTab === "graph" ? "" : "is-hidden"}`}>
              <div className="graph" ref={containerRef} />
              {!graph && <p className="graph-empty">Select an output to build the graph.</p>}
            </div>
            <div className={`machines-panel ${graphTab === "machines" ? "" : "is-hidden"}`}>
              {recipeStats.length === 0 && <p className="empty">No machine stats yet.</p>}
              {recipeStats.map(node => (
                <div key={node.id} className="machine-card">
                  <strong>{node.label}</strong>
                  <span>
                    {node.machines === undefined || Number.isNaN(node.machines)
                      ? "?"
                      : Number.isInteger(node.machines)
                        ? node.machines
                        : node.machines.toFixed(1)}{" "}
                    machines
                  </span>
                  {node.rid && selectedRecipe?.min_tier && (
                    <div className="machine-tier">
                      <label>
                        Tier
                        <select
                          value={recipeTierOverrides[node.rid] || selectedRecipe.min_tier}
                          onChange={e => {
                            const nextTier = e.target.value
                            setRecipeTierOverrides(prev => ({ ...prev, [node.rid as string]: nextTier }))
                            setRecipeOverclockTiers(prev => ({
                              ...prev,
                              [node.rid as string]: getOverclockTiers(selectedRecipe.min_tier, nextTier)
                            }))
                          }}
                        >
                          {getTierOptions(selectedRecipe.min_tier).map(tier => (
                            <option key={tier} value={tier}>
                              {tier}
                            </option>
                          ))}
                        </select>
                      </label>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>

        <aside className="config-panel">
          <div className="panel-tabs">
            <button
              className={configTab === "outputs" ? "active" : ""}
              onClick={() => setConfigTab("outputs")}
            >
              Outputs
            </button>
            <button
              className={configTab === "inputs" ? "active" : ""}
              onClick={() => setConfigTab("inputs")}
            >
              Inputs
            </button>
            <button
              className={configTab === "options" ? "active" : ""}
              onClick={() => setConfigTab("options")}
            >
              Options
            </button>
          </div>
          <div className="panel-body">
            <div className={`output-panel ${configTab === "outputs" ? "" : "is-hidden"}`}>
              {!outputTarget && <p className="empty">No output selected.</p>}
              {outputTarget && (
                <div className="output-summary">
                  <strong>{outputTarget.name || outputTarget.id}</strong>
                  <span>meta {outputTarget.meta}</span>
                  {selectedRecipe && (
                    <small>
                      {selectedRecipe.machine_name || selectedRecipe.machine_id} | {selectedRecipe.rid.split(":").pop()}
                    </small>
                  )}
                </div>
              )}
              <button onClick={openOutputModal}>
                {outputTarget ? "Change output" : "Select output"}
              </button>
              {error && <p className="error">{error}</p>}
            </div>
            <div className={`inputs-panel ${configTab === "inputs" ? "" : "is-hidden"}`}>
              {!selectedRecipe && <p className="empty">Select an output to view inputs.</p>}
              {selectedRecipe && (
                <>
                  <p className="inputs-title">Inputs</p>
                  {selectedRecipe.item_inputs?.map(item => (
                    <div key={`${item.item_id}:${item.meta}`} className="input-row">
                      <span>{item.name || item.item_id}</span>
                      <small>x{item.count}</small>
                    </div>
                  ))}
                  {selectedRecipe.fluid_inputs?.map(fluid => (
                    <div key={fluid.fluid_id} className="input-row">
                      <span>{fluid.name || fluid.fluid_id}</span>
                      <small>{fluid.mb} mb</small>
                    </div>
                  ))}
                </>
              )}
            </div>
            <div className={`options-panel ${configTab === "options" ? "" : "is-hidden"}`}>
              <label>
                Output per minute
                <input
                  type="number"
                  min="0"
                  step="0.1"
                  value={ratePerMin}
                  onChange={e => setRatePerMin(Number(e.target.value))}
                />
              </label>
              <label>
                Overclock tiers
                <input
                  type="number"
                  min="0"
                  step="1"
                  value={overclock}
                  onChange={e => setOverclock(Number(e.target.value))}
                />
              </label>
              <label>
                Parallel
                <input
                  type="number"
                  min="1"
                  step="1"
                  value={parallel}
                  onChange={e => setParallel(Number(e.target.value))}
                />
              </label>
              <button onClick={runGraph}>Build graph</button>
            </div>
          </div>
        </aside>
      </section>

      {showOutputModal && (
        <div className="modal-backdrop">
          <div className="modal">
            <div className="modal-header">
              <h2>Select Output</h2>
              {outputTarget && (
                <button className="modal-close" onClick={() => setShowOutputModal(false)}>
                  Close
                </button>
              )}
            </div>
            {!selectedOutput && (
              <div className="modal-section">
                <p className="modal-label">Search outputs</p>
                <input
                  placeholder="Search items or fluids..."
                  value={outputQuery}
                  onChange={e => setOutputQuery(e.target.value)}
                />
                {isLoadingOutputs && <p className="empty">Searching outputs...</p>}
                <div className="output-results">
                  {outputResults.map(result => (
                    <button
                      key={`${result.type}:${result.id}:${result.meta}`}
                      className="output-result"
                      onClick={() => {
                        setSelectedOutput(result)
                        setSelectedMachineId(null)
                      }}
                    >
                      <span>{result.name || result.id}</span>
                      <small>{result.type === "item" ? `meta ${result.meta}` : "fluid"}</small>
                    </button>
                  ))}
                </div>
                {!isLoadingOutputs && outputResults.length === 0 && outputQuery.trim().length >= 2 && (
                  <p className="empty">No outputs found.</p>
                )}
              </div>
            )}
            {selectedOutput && !selectedMachineId && (
              <div className="modal-section">
                <div className="modal-toolbar">
                  <button
                    className="modal-back"
                    onClick={() => {
                      setSelectedOutput(null)
                      setOutputRecipes([])
                    }}
                  >
                    Back
                  </button>
                  <div className="selected-output">
                    <strong>{selectedOutput.name || selectedOutput.id}</strong>
                    <small>{selectedOutput.type === "item" ? `meta ${selectedOutput.meta}` : "fluid"}</small>
                  </div>
                </div>
                {isLoadingMachines && <p className="empty">Loading machines...</p>}
                <div className="machine-list">
                  {machinesForOutput.map(machine => (
                    <button
                      key={machine.machine_id}
                      onClick={() => setSelectedMachineId(machine.machine_id)}
                    >
                      <span>{machine.machine_name}</span>
                      <small>{machine.machine_id}</small>
                    </button>
                  ))}
                </div>
                {!isLoadingMachines && machinesForOutput.length === 0 && (
                  <p className="empty">No machines found.</p>
                )}
              </div>
            )}
            {selectedOutput && selectedMachineId && (
              <div className="modal-section">
                <div className="modal-toolbar">
                  <button className="modal-back" onClick={() => setSelectedMachineId(null)}>
                    Back
                  </button>
                  <div className="selected-output">
                    <strong>{selectedOutput.name || selectedOutput.id}</strong>
                    <small>{selectedOutput.type === "item" ? `meta ${selectedOutput.meta}` : "fluid"}</small>
                  </div>
                  <div className="selected-machine">
                    <span>
                      {(machinesForOutput.find(machine => machine.machine_id === selectedMachineId)?.machine_name) ||
                        selectedMachineId}
                    </span>
                    <button
                      className="selected-machine-reset"
                      onClick={() => setSelectedMachineId(null)}
                    >
                      Change
                    </button>
                  </div>
                </div>
                {isLoadingRecipes && <p className="empty">Loading recipes...</p>}
                <div className="output-grid">
                  {machineGroups
                    .find(group => group.machine_id === selectedMachineId)
                    ?.recipes.map(recipe => {
                      const selectedTier = recipeTierOverrides[recipe.rid] || recipe.min_tier
                      return (
                        <div key={recipe.rid} className="output-card">
                          <div className="output-card-header">
                            <div>
                              <strong>{recipe.machine_name || recipe.machine_id}</strong>
                              <small>{recipe.rid.split(":").pop()}</small>
                            </div>
                            <div className="output-card-actions">
                              <label>
                                Tier
                                <select
                                  value={selectedTier || ""}
                                  onChange={e => {
                                    const nextTier = e.target.value
                                    setRecipeTierOverrides(prev => ({
                                      ...prev,
                                      [recipe.rid]: nextTier
                                    }))
                                    setRecipeOverclockTiers(prev => ({
                                      ...prev,
                                      [recipe.rid]: getOverclockTiers(recipe.min_tier, nextTier)
                                    }))
                                  }}
                                >
                                  {getTierOptions(recipe.min_tier).map(tier => (
                                    <option key={tier} value={tier}>
                                      {tier}
                                    </option>
                                  ))}
                                </select>
                              </label>
                              <button
                                className="output-card-select"
                                onClick={() => applyOutputSelection(selectedOutput, recipe)}
                              >
                                Select
                              </button>
                            </div>
                          </div>
                          <div className="output-card-io">
                            <div>
                              <p className="output-io-title">Inputs</p>
                              {(recipe.item_inputs ?? []).map(item => (
                                <div key={`${item.item_id}:${item.meta}`} className="output-io-row">
                                  <span>{item.name || item.item_id}</span>
                                  <small>x{item.count}</small>
                                </div>
                              ))}
                              {(recipe.fluid_inputs ?? []).map(fluid => (
                                <div key={fluid.fluid_id} className="output-io-row">
                                  <span>{fluid.name || fluid.fluid_id}</span>
                                  <small>{fluid.mb} mb</small>
                                </div>
                              ))}
                            </div>
                            <div>
                              <p className="output-io-title">Outputs</p>
                              {(recipe.item_outputs ?? []).map(item => (
                                <div key={`${item.item_id}:${item.meta}`} className="output-io-row">
                                  <span>{item.name || item.item_id}</span>
                                  <small>x{item.count}</small>
                                </div>
                              ))}
                              {(recipe.fluid_outputs ?? []).map(fluid => (
                                <div key={fluid.fluid_id} className="output-io-row">
                                  <span>{fluid.name || fluid.fluid_id}</span>
                                  <small>{fluid.mb} mb</small>
                                </div>
                              ))}
                            </div>
                          </div>
                        </div>
                      )
                    })}
                  {!isLoadingRecipes &&
                    machineGroups.find(group => group.machine_id === selectedMachineId)?.recipes
                      .length === 0 && <p className="empty">No recipes found.</p>}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
