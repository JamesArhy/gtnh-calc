import { useEffect, useMemo, useRef, useState } from "react"
import cytoscape from "cytoscape"
import type { GraphResponse, RecipeIOFluid, RecipeIOItem } from "./types"
import { fetchGraph, fetchRecipesByOutput, searchItems } from "./api"

const DEFAULT_TARGET = {
  type: "item",
  id: "item:gregtech:gt.metaitem.01",
  meta: 0
}
const MAX_SEARCH_RESULTS = 20
const SEARCH_DEBOUNCE_MS = 300
const TIERS = ["ULV", "LV", "MV", "HV", "EV", "IV", "LuV", "ZPM", "UV", "UHV"]
const RECIPE_RESULTS_LIMIT = 200

type Target = typeof DEFAULT_TARGET & { name?: string }

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

  const [targets, setTargets] = useState<Target[]>([])
  const [activeTargetIndex, setActiveTargetIndex] = useState(0)
  const [ratePerMin, setRatePerMin] = useState(1)
  const [overclock, setOverclock] = useState(0)
  const [parallel, setParallel] = useState(1)
  const [query, setQuery] = useState("")
  const [results, setResults] = useState<any[]>([])
  const [hasSearched, setHasSearched] = useState(false)
  const [isSearching, setIsSearching] = useState(false)
  const [tooManyResults, setTooManyResults] = useState(false)
  const [recipes, setRecipes] = useState<RecipeOption[]>([])
  const [recipeOverrides, setRecipeOverrides] = useState<Record<string, string>>({})
  const [recipeTierOverrides, setRecipeTierOverrides] = useState<Record<string, string>>({})
  const [recipeOverclockTiers, setRecipeOverclockTiers] = useState<Record<string, number>>({})
  const [recipeSelections, setRecipeSelections] = useState<
    Record<string, { rid: string; optionLabel: string; machineName?: string; tier?: string }>
  >({})
  const [recipeMetaByRid, setRecipeMetaByRid] = useState<Record<string, { min_tier?: string }>>({})
  const [expandedMachines, setExpandedMachines] = useState<Record<string, boolean>>({})
  const [expandedRecipes, setExpandedRecipes] = useState<Record<string, boolean>>({})
  const [graph, setGraph] = useState<GraphResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  const activeTarget = targets[activeTargetIndex]
  const targetKey = (target: Target) =>
    target.type === "item" ? `item:${target.id}:${target.meta}` : `fluid:${target.id}`
  const activeTargetKey = activeTarget ? targetKey(activeTarget) : null
  const selectedRecipeRid = activeTargetKey ? recipeOverrides[activeTargetKey] ?? null : null

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
    const formatDuration = (ticks?: number) => {
      if (!ticks) return "?"
      return `${(ticks / 20).toFixed(2)}s`
    }
    const formatMachines = (value?: number) => {
      if (value === undefined || Number.isNaN(value)) return "?"
      return Number.isInteger(value) ? String(value) : value.toFixed(1)
    }
    const formatRecipeLabel = (node: any) => {
      const name = node.machine_name || node.machine_id || node.label
      const machines = formatMachines(node.machines_required)
      const rid = node.rid as string | undefined
      const tier =
        (rid && recipeTierOverrides[rid]) ||
        (rid && recipeMetaByRid[rid]?.min_tier) ||
        "?"
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
  }, [graph])

  const recipeStats = useMemo(() => {
    if (!graph) return []
    return graph.nodes
      .filter(node => node.type === "recipe")
      .map(node => ({
        id: node.id,
        rid: node.rid,
        label: node.label,
        machines: node.machines_required,
        rate: node.per_machine_rate_per_s,
        duration_ticks: node.duration_ticks,
        eut: node.eut,
        overclock_tiers: node.overclock_tiers
      }))
  }, [graph])

  const groupedRecipes = useMemo(() => {
    const grouped = new Map<string, RecipeOption[]>()
    for (const recipe of recipes) {
      const groupKey = recipe.machine_name || recipe.machine_id
      const list = grouped.get(groupKey) ?? []
      list.push(recipe)
      grouped.set(groupKey, list)
    }
    const groups = Array.from(grouped.entries()).map(([machine_id, list]) => {
      const sorted = [...list].sort((a, b) => a.eut - b.eut || a.duration_ticks - b.duration_ticks)
      return { machine_id, recipes: sorted, minEut: sorted[0]?.eut ?? 0 }
    })
    return groups.sort((a, b) => a.minEut - b.minEut || a.machine_id.localeCompare(b.machine_id))
  }, [recipes])

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

  const updateActiveTarget = (nextTarget: Target) => {
    setTargets(prev => {
      if (prev.length === 0) {
        setActiveTargetIndex(0)
        return [nextTarget]
      }
      return prev.map((target, index) => (index === activeTargetIndex ? nextTarget : target))
    })
  }

  const addTargetFromSearch = (nextTarget: Target) => {
    setTargets(prev => {
      const next = [...prev, nextTarget]
      setActiveTargetIndex(next.length - 1)
      return next
    })
  }

  const addTarget = () => {
    if (!activeTarget) return
    setTargets(prev => {
      const next = [...prev, activeTarget]
      setActiveTargetIndex(next.length - 1)
      return next
    })
  }

  const removeTarget = (index: number) => {
    setTargets(prev => {
      if (prev.length === 0) return prev
      const next = prev.filter((_, i) => i !== index)
      if (next.length === 0) {
        setActiveTargetIndex(0)
        return next
      }
      if (activeTargetIndex >= next.length) {
        setActiveTargetIndex(next.length - 1)
      } else if (index === activeTargetIndex) {
        setActiveTargetIndex(Math.max(0, index - 1))
      }
      return next
    })
  }

  useEffect(() => {
    const trimmed = query.trim()
    if (trimmed.length < 2) {
      setResults([])
      setHasSearched(false)
      setTooManyResults(false)
      return
    }
    const handle = setTimeout(() => {
      runSearch(trimmed)
    }, SEARCH_DEBOUNCE_MS)
    return () => clearTimeout(handle)
  }, [query])

  async function runSearch(nextQuery?: string) {
    const trimmed = (nextQuery ?? query).trim()
    if (!trimmed) return
    setHasSearched(true)
    setIsSearching(true)
    const data = await searchItems(trimmed, MAX_SEARCH_RESULTS + 1)
    const items = data.items || []
    if (items.length > MAX_SEARCH_RESULTS) {
      setResults([])
      setTooManyResults(true)
    } else {
      setTooManyResults(false)
      setResults(items)
    }
    setIsSearching(false)
  }

  useEffect(() => {
    const loadRecipes = async () => {
      if (!activeTarget?.id) {
        setRecipes([])
        return
      }
      const data = await fetchRecipesByOutput({
        output_type: activeTarget.type as "item" | "fluid",
        item_id: activeTarget.type === "item" ? activeTarget.id : undefined,
        meta: activeTarget.type === "item" ? activeTarget.meta : undefined,
        fluid_id: activeTarget.type === "fluid" ? activeTarget.id : undefined,
        limit: RECIPE_RESULTS_LIMIT
      })
      const nextRecipes = data.recipes || []
      setRecipes(nextRecipes)
      setRecipeMetaByRid(prev => {
        const next = { ...prev }
        for (const recipe of nextRecipes) {
          next[recipe.rid] = { min_tier: recipe.min_tier }
        }
        return next
      })
    }
    loadRecipes()
  }, [activeTarget])

  async function runGraph() {
    setError(null)
    try {
      const recipe_override: Record<string, string> = { ...recipeOverrides }
      if (targets.length === 0) {
        setError("Select a target before building the graph")
        return
      }
      const payload = {
        targets: targets.map(target => ({
          target_type: target.type,
          target_id: target.id,
          target_meta: target.meta,
            target_rate_per_s: ratePerMin / 60
        })),
        max_depth: 0,
        overclock_tiers: overclock,
        parallel: parallel,
        recipe_override,
        recipe_overclock_tiers: recipeOverclockTiers
      }
      const data = await fetchGraph(payload)
      setGraph(data)
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
          <div>
            <span className="stat-label">Targets</span>
            <span className="stat-value">{targets.length}</span>
          </div>
        </div>
      </header>

      <section className="controls">
        <div className="control-card">
          <h2>Target output</h2>
          <div className="control-row">
            <input
              placeholder="Search item or id..."
              value={query}
              onChange={e => setQuery(e.target.value)}
            />
            <button onClick={() => runSearch()} disabled={isSearching}>
              {isSearching ? "Searching..." : "Search"}
            </button>
          </div>
          <div className="results">
            {tooManyResults && (
              <p className="empty">Too many matches. Keep typing.</p>
            )}
            {!tooManyResults &&
              results.map(item => (
                <button
                  key={`${item.item_id}:${item.meta}`}
                  className={
                    item.item_id === activeTarget?.id && item.meta === activeTarget?.meta ? "active" : ""
                  }
                  onClick={() =>
                    addTargetFromSearch({
                      type: "item",
                      id: item.item_id,
                      meta: item.meta,
                      name: item.name
                    })
                  }
                >
                  <span>{item.name || item.item_id}</span>
                  <small>meta {item.meta}</small>
                </button>
              ))}
            {hasSearched && !tooManyResults && results.length === 0 && (
              <p className="empty">No matches found.</p>
            )}
          </div>
          <div className="targets">
            <p className="targets-title">Targets</p>
            <div className="targets-list">
              {targets.map((target, index) => {
                const key = targetKey(target)
                const selection = recipeSelections[key]
                return (
                  <div
                    key={`${target.id}:${target.meta}:${index}`}
                    className={`target-pill ${index === activeTargetIndex ? "active" : ""}`}
                  >
                    <button className="target-select" onClick={() => setActiveTargetIndex(index)}>
                      <span>{target.name || target.id}</span>
                      <small>meta {target.meta}</small>
                      {selection && (
                        <small>
                          {selection.optionLabel} | {selection.machineName}
                          {selection.tier ? ` | ${selection.tier}` : ""}
                        </small>
                      )}
                    </button>
                    <button
                      className="target-remove"
                      onClick={() => removeTarget(index)}
                    >
                      Remove
                    </button>
                  </div>
                )
              })}
            {targets.length === 0 && <p className="empty">No targets selected.</p>}
          </div>
            <button className="target-add" onClick={addTarget} disabled={!activeTarget}>
              Add target
            </button>
          </div>
          <div className="recipes">
            <p className="recipes-title">Recipe options</p>
            {recipes.length === 0 && <p className="empty">No recipes found.</p>}
            {groupedRecipes.map(group => {
              const isExpanded = expandedMachines[group.machine_id] ?? false
              return (
                <div key={group.machine_id} className="recipe-group">
                  <button
                    className="recipe-group-toggle"
                    onClick={() =>
                      setExpandedMachines(prev => ({
                        ...prev,
                        [group.machine_id]: !isExpanded
                      }))
                    }
                  >
                    <span>{group.machine_id}</span>
                      <small>
                        {group.recipes.length} option{group.recipes.length === 1 ? "" : "s"} | {group.minEut} EU/t min
                      </small>
                  </button>
                  {isExpanded &&
                    group.recipes.map((recipe, index) => {
                      const duration = (recipe.duration_ticks / 20).toFixed(2)
                      const isActive = selectedRecipeRid === recipe.rid
                      const ridSuffix = recipe.rid.split(":").pop() || recipe.rid
                      const ridTail = ridSuffix.slice(-4).toUpperCase()
                      const optionLabel = `Option ${index + 1} (${ridTail})`
                      const selectedTier = recipeTierOverrides[recipe.rid] || recipe.min_tier
                      const overclockTiers = getOverclockTiers(recipe.min_tier, selectedTier)
                      const isRecipeExpanded = expandedRecipes[recipe.rid] ?? false
                      const itemInputs = recipe.item_inputs ?? []
                      const fluidInputs = recipe.fluid_inputs ?? []
                      const itemOutputs = recipe.item_outputs ?? []
                      const fluidOutputs = recipe.fluid_outputs ?? []
                      return (
                        <div key={recipe.rid} className={`recipe-option ${isActive ? "active" : ""}`}>
                          <div className="recipe-option-header">
                            <button
                              className="recipe-option-select"
                              onClick={() =>
                                {
                                  setRecipeOverrides(prev => {
                                    if (!activeTargetKey) return prev
                                    return { ...prev, [activeTargetKey]: recipe.rid }
                                  })
                                  if (activeTargetKey) {
                                    setRecipeSelections(prev => ({
                                      ...prev,
                                      [activeTargetKey]: {
                                        rid: recipe.rid,
                                        optionLabel,
                                        machineName: group.machine_id,
                                        tier: selectedTier
                                      }
                                    }))
                                  }
                                  if (selectedTier) {
                                    setRecipeTierOverrides(prev => ({
                                      ...prev,
                                      [recipe.rid]: selectedTier
                                    }))
                                    setRecipeOverclockTiers(prev => ({
                                      ...prev,
                                      [recipe.rid]: overclockTiers
                                    }))
                                  }
                                }
                              }
                            >
                              <span>{optionLabel}</span>
                              <small>
                                {duration}s | {recipe.eut} EU/t
                              </small>
                            </button>
                            <button
                              className="recipe-option-toggle"
                              onClick={() =>
                                setExpandedRecipes(prev => ({
                                  ...prev,
                                  [recipe.rid]: !isRecipeExpanded
                                }))
                              }
                            >
                              {isRecipeExpanded ? "Hide" : "Details"}
                            </button>
                          </div>
                          {isRecipeExpanded && (
                            <div className="recipe-io">
                              <div>
                                <p className="recipe-io-title">Inputs</p>
                                {itemInputs.length === 0 && fluidInputs.length === 0 && (
                                  <p className="empty">No inputs.</p>
                                )}
                                {itemInputs.map(item => (
                                  <div key={`${item.item_id}:${item.meta}`} className="recipe-io-row">
                                    <span>{item.name || item.item_id}</span>
                                    <small>x{item.count}</small>
                                  </div>
                                ))}
                                {fluidInputs.map(fluid => (
                                  <div key={fluid.fluid_id} className="recipe-io-row">
                                    <span>{fluid.name || fluid.fluid_id}</span>
                                    <small>{fluid.mb} mb</small>
                                  </div>
                                ))}
                              </div>
                              <div>
                                <p className="recipe-io-title">Outputs</p>
                                {itemOutputs.length === 0 && fluidOutputs.length === 0 && (
                                  <p className="empty">No outputs.</p>
                                )}
                                {itemOutputs.map(item => (
                                  <div key={`${item.item_id}:${item.meta}`} className="recipe-io-row">
                                    <span>{item.name || item.item_id}</span>
                                    <small>x{item.count}</small>
                                  </div>
                                ))}
                                {fluidOutputs.map(fluid => (
                                  <div key={fluid.fluid_id} className="recipe-io-row">
                                    <span>{fluid.name || fluid.fluid_id}</span>
                                    <small>{fluid.mb} mb</small>
                                  </div>
                                ))}
                              </div>
                              <div className="recipe-tier">
                                <p className="recipe-io-title">Machine tier</p>
                                <select
                                  value={selectedTier || ""}
                                  onChange={e => {
                                    const nextTier = e.target.value
                                    setRecipeTierOverrides(prev => ({ ...prev, [recipe.rid]: nextTier }))
                                    setRecipeOverclockTiers(prev => ({
                                      ...prev,
                                      [recipe.rid]: getOverclockTiers(recipe.min_tier, nextTier)
                                    }))
                                    if (activeTargetKey && selectedRecipeRid === recipe.rid) {
                                      setRecipeSelections(prev => ({
                                        ...prev,
                                        [activeTargetKey]: {
                                          rid: recipe.rid,
                                          optionLabel,
                                          machineName: group.machine_id,
                                          tier: nextTier
                                        }
                                      }))
                                    }
                                  }}
                                >
                                  <option value="" disabled>
                                    Select tier
                                  </option>
                                  {getTierOptions(recipe.min_tier).map(tier => (
                                    <option key={tier} value={tier}>
                                      {tier}
                                    </option>
                                  ))}
                                </select>
                                <small>Overclocks: {overclockTiers}</small>
                              </div>
                            </div>
                          )}
                        </div>
                      )
                    })}
                </div>
              )
            })}
          </div>
        </div>

        <div className="control-card">
          <h2>Machine tuning</h2>
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
          {error && <p className="error">{error}</p>}
        </div>

        <div className="control-card">
          <h2>Machine counts</h2>
          <div className="machines">
            {recipeStats.map(node => (
              <div key={node.id}>
                <strong>{node.label}</strong>
                <span>
                  {node.machines === undefined || Number.isNaN(node.machines)
                    ? "?"
                    : Number.isInteger(node.machines)
                      ? node.machines
                      : node.machines.toFixed(1)}{" "}
                  machines
                </span>
                {node.rid && recipeMetaByRid[node.rid]?.min_tier && (
                  <div className="machine-tier">
                    <label>
                      Tier
                      <select
                        value={recipeTierOverrides[node.rid] || recipeMetaByRid[node.rid].min_tier}
                        onChange={e => {
                          const nextTier = e.target.value
                          setRecipeTierOverrides(prev => ({ ...prev, [node.rid as string]: nextTier }))
                          setRecipeOverclockTiers(prev => ({
                            ...prev,
                            [node.rid as string]: getOverclockTiers(
                              recipeMetaByRid[node.rid as string]?.min_tier,
                              nextTier
                            )
                          }))
                          setRecipeSelections(prev => {
                            const next = { ...prev }
                            for (const key of Object.keys(next)) {
                              if (next[key]?.rid === node.rid) {
                                next[key] = { ...next[key], tier: nextTier }
                              }
                            }
                            return next
                          })
                        }}
                      >
                        {getTierOptions(recipeMetaByRid[node.rid]?.min_tier).map(tier => (
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
      </section>

      <section className="graph-section">
        <div className="graph" ref={containerRef} />
      </section>
    </div>
  )
}
