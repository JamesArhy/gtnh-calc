import { useEffect, useMemo, useRef, useState } from "react"
import cytoscape from "cytoscape"
import cytoscapeSvg from "cytoscape-svg"
import { jsPDF } from "jspdf"
import { svg2pdf } from "svg2pdf.js"
import type { GraphResponse, RecipeIOFluid, RecipeIOItem } from "./types"
import { fetchGraph, fetchMachinesByOutput, fetchRecipesByOutput, searchFluids, searchItems } from "./api"

cytoscape.use(cytoscapeSvg)

const SEARCH_DEBOUNCE_MS = 300
const OUTPUT_SEARCH_LIMIT = 30
const TIERS = ["ULV", "LV", "MV", "HV", "EV", "IV", "LuV", "ZPM", "UV", "UHV"]
const RECIPE_RESULTS_LIMIT = 200
const TIER_COLORS: Record<string, string> = {
  ULV: "#4DBE6B",
  LV: "#4DBE6B",
  MV: "#E4C14A",
  HV: "#E48A3A",
  EV: "#E05555",
  IV: "#8E60D5",
  LuV: "#C856C9",
  ZPM: "#C856C9",
  UV: "#C856C9",
  UHV: "#C856C9"
}
const TIER_CAPS: Record<string, number> = {
  ULV: 8,
  LV: 32,
  MV: 128,
  HV: 512,
  EV: 2048,
  IV: 8192,
  LuV: 32768,
  ZPM: 131072,
  UV: 524288,
  UHV: 2097152
}
const GAS_FLUID_IDS = new Set<string>()
const COIL_TYPES = [
  { id: "Any", label: "Any", maxTemp: Infinity },
  { id: "Cupronickel", label: "Cupronickel", maxTemp: 1801 },
  { id: "Kanthal", label: "Kanthal", maxTemp: 2701 },
  { id: "Nichrome", label: "Nichrome", maxTemp: 3601 },
  { id: "TPV-Alloy", label: "TPV-Alloy", maxTemp: 4501 },
  { id: "HSS-G", label: "HSS-G", maxTemp: 5401 },
  { id: "HSS-S", label: "HSS-S", maxTemp: 6301 },
  { id: "Naquadah", label: "Naquadah", maxTemp: 7201 },
  { id: "Naquadah Alloy", label: "Naquadah Alloy", maxTemp: 8101 },
  { id: "Trinium", label: "Trinium", maxTemp: 9001 },
  { id: "Electrum Flux", label: "Electrum Flux", maxTemp: 9901 },
  { id: "Awakened Draconium", label: "Awakened Draconium", maxTemp: 10801 },
  { id: "Infinity", label: "Infinity", maxTemp: 11701 },
  { id: "Hypogen", label: "Hypogen", maxTemp: 12601 },
  { id: "Eternal", label: "Eternal", maxTemp: 13501 }
]

type Target = {
  type: "item" | "fluid"
  id: string
  meta: number
  name?: string
}

type InputTarget = {
  key: string
  target: Target
  rate_per_s: number
  recipe?: RecipeOption
}

type MachineOption = {
  machine_id: string
  machine_name: string
  recipe_count?: number
}

type RecipeOption = {
  rid: string
  machine_id: string
  machine_name?: string
  base_duration_ticks?: number
  base_eut?: number
  duration_ticks: number
  eut: number
  min_tier?: string
  min_voltage?: number
  amps?: number
  ebf_temp?: number
  item_inputs?: RecipeIOItem[]
  fluid_inputs?: RecipeIOFluid[]
  item_outputs?: RecipeIOItem[]
  fluid_outputs?: RecipeIOFluid[]
}

type SavedConfig = {
  outputTarget: Target | null
  selectedRecipe: RecipeOption | null
  inputTargets: InputTarget[]
  rateValue: number
  rateUnit: "min" | "sec"
  userVoltageTier: string
  maxCoilType: string
  recipeTierOverrides: Record<string, string>
  recipeOverclockTiers: Record<string, number>
  inputRecipeOverrides: Record<string, string>
  machineTierSelections: Record<string, string>
}

type SavedConfigEntry = {
  id: string
  name: string
  savedAt: string
  config: SavedConfig
}

export default function App() {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const graphSectionRef = useRef<HTMLDivElement | null>(null)
  const cyRef = useRef<cytoscape.Core | null>(null)

  const [outputTarget, setOutputTarget] = useState<Target | null>(null)
  const [selectedRecipe, setSelectedRecipe] = useState<RecipeOption | null>(null)
  const [rateValue, setRateValue] = useState(1)
  const [rateUnit, setRateUnit] = useState<"min" | "sec">("min")
  const [userVoltageTier, setUserVoltageTier] = useState<string>("Any")
  const [maxCoilType, setMaxCoilType] = useState<string>("Any")
  const lastTargetRatePerSRef = useRef<number>(rateUnit === "min" ? rateValue / 60 : rateValue)
  const isScalingInputsRef = useRef(false)
  const [recipeTierOverrides, setRecipeTierOverrides] = useState<Record<string, string>>({})
  const [recipeOverclockTiers, setRecipeOverclockTiers] = useState<Record<string, number>>({})
  const [inputTargets, setInputTargets] = useState<InputTarget[]>([])
  const [inputRecipeOverrides, setInputRecipeOverrides] = useState<Record<string, string>>({})

  const [graph, setGraph] = useState<GraphResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [graphTab, setGraphTab] = useState<"graph" | "machines">("graph")
  const [configTab, setConfigTab] = useState<"outputs" | "inputs" | "options" | "saved">("outputs")

  const [showOutputModal, setShowOutputModal] = useState(false)
  const [outputQuery, setOutputQuery] = useState("")
  const [outputResults, setOutputResults] = useState<Target[]>([])
  const [selectedOutput, setSelectedOutput] = useState<Target | null>(null)
  const [outputRecipes, setOutputRecipes] = useState<RecipeOption[]>([])
  const [selectedMachineId, setSelectedMachineId] = useState<string | null>(null)
  const [isLoadingOutputs, setIsLoadingOutputs] = useState(false)
  const [isLoadingMachines, setIsLoadingMachines] = useState(false)
  const [isLoadingRecipes, setIsLoadingRecipes] = useState(false)
  const [machinesForOutput, setMachinesForOutput] = useState<MachineOption[]>([])
  const [recentRecipes, setRecentRecipes] = useState<{ target: Target; recipe: RecipeOption }[]>(() => {
    const key = "gtnh_recent_recipes_v1"
    try {
      const raw = window.localStorage.getItem(key)
      if (!raw) return []
      const parsed = JSON.parse(raw)
      return Array.isArray(parsed) ? parsed.slice(0, 5) : []
    } catch (err) {
      console.warn("Failed to load recent recipes", err)
      return []
    }
  })
  const [recipeQuery, setRecipeQuery] = useState("")
  const [hoverInfo, setHoverInfo] = useState<{
    x: number
    y: number
    lines: string[]
  } | null>(null)
  const [showFlowAnimation, setShowFlowAnimation] = useState(false)
  const [radialMenu, setRadialMenu] = useState<
    | {
        kind: "target"
        x: number
        y: number
        target: Target
        isOutput: boolean
        ratePerS: number | null
      }
    | {
        kind: "recipe"
        x: number
        y: number
        rid: string
        minTier?: string
      }
    | null
  >(null)
  const graphRunTimerRef = useRef<number | null>(null)
  const isRestoringConfigRef = useRef(false)
  const [restoreVersion, setRestoreVersion] = useState(0)
  const [selectionMode, setSelectionMode] = useState<"output" | "input">("output")
  const [pendingInputRate, setPendingInputRate] = useState<number | null>(null)
  const [machineTierSelections, setMachineTierSelections] = useState<Record<string, string>>({})
  const [savedConfigs, setSavedConfigs] = useState<SavedConfigEntry[]>(() => {
    const key = "gtnh_saved_configs_v1"
    try {
      const raw = window.localStorage.getItem(key)
      if (!raw) return []
      const parsed = JSON.parse(raw)
      return Array.isArray(parsed) ? parsed : []
    } catch (err) {
      console.warn("Failed to load saved configs", err)
      return []
    }
  })
  const [configName, setConfigName] = useState("")

  const getTargetKey = (target: Target) =>
    target.type === "item" ? `item:${target.id}:${target.meta}` : `fluid:${target.id}`

  const getRecipeNodeId = (rid: string, outputKeyValue: string) => `recipe:${rid}:${outputKeyValue}`

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
            "background-color": "#B08A57",
            "background-opacity": 0.9,
            "border-width": 2,
            "border-color": "#8D6A46",
            "label": "data(label)",
            "color": "#f4efe6",
            "font-family": "Space Grotesk, sans-serif",
            "font-weight": "600",
            "font-size": "11px",
            "text-valign": "bottom",
            "text-halign": "center",
            "text-wrap": "wrap",
            "text-max-width": "140px",
            "text-margin-y": 10,
            "text-background-color": "#141312",
            "text-background-opacity": 0.7,
            "text-background-padding": "3px",
            "text-background-shape": "round-rectangle",
            "width": 52,
            "height": 52,
            "shape": "round-hexagon"
          }
        },
        {
          selector: "node[type = \"recipe\"]",
          style: {
            "background-color": "#2E4A62",
            "background-fill": "linear-gradient",
            "background-gradient-stop-colors": "#3A5E79 #253D54",
            "shape": "round-rectangle",
            "corner-radius": 10,
            "border-width": 2,
            "border-color": "data(tier_color)",
            "width": 76,
            "height": 60,
            "text-max-width": "160px",
            "transition-property": "border-color border-width",
            "transition-duration": 150
          }
        },
        {
          selector: "node[type = \"fluid\"]",
          style: {
            "background-color": "#4FA3D1",
            "shape": "round-rectangle",
            "corner-radius": 18,
            "width": 68,
            "height": 36,
            "border-color": "#2C6D8F",
            "border-width": 2
          }
        },
        {
          selector: "node[type = \"gas\"]",
          style: {
            "background-color": "#6EDDD8",
            "shape": "hexagon",
            "border-color": "#3BAFA9",
            "border-width": 2
          }
        },
        {
          selector: "edge",
          style: {
            "line-color": "#C9A26B",
            "target-arrow-color": "#C9A26B",
            "target-arrow-shape": "triangle",
            "curve-style": "straight",
            "width": "data(edge_width)",
            "line-opacity": 0.55,
            "arrow-scale": 0.8,
            "label": "data(label)",
            "font-family": "Space Grotesk, sans-serif",
            "font-size": "10px",
            "color": "#f4efe6",
            "text-background-color": "#1b1713",
            "text-background-opacity": 0.25,
            "text-background-padding": "1px",
            "text-background-shape": "round-rectangle",
            "text-outline-color": "#1b1713",
            "text-outline-width": 1,
            "text-rotation": "autorotate",
            "text-margin-y": -10,
            "line-style": "solid"
          }
        },
        {
          selector: "edge[material_state = \"fluid\"]",
          style: {
            "line-color": "#4FA3D1",
            "target-arrow-color": "#4FA3D1",
            "line-cap": "round"
          }
        },
        {
          selector: "edge[material_state = \"gas\"]",
          style: {
            "line-color": "#6EDDD8",
            "target-arrow-color": "#6EDDD8",
            "line-style": "dashed",
            "line-dash-pattern": [5, 4],
            "line-cap": "round"
          }
        },
        {
          selector: "edge[active = \"true\"]",
          style: {
            "opacity": 1,
            "line-opacity": 0.8,
            "width": "data(edge_width)"
          }
        },
        {
          selector: "edge[active = \"false\"]",
          style: {
            "opacity": 0.4,
            "line-opacity": 0.3
          }
        },
        {
          selector: "edge[bottleneck = \"true\"]",
          style: {
            "line-color": "#E05555",
            "target-arrow-color": "#E05555",
            "line-opacity": 0.9,
            "width": "data(edge_width)"
          }
        },
        {
          selector: ".pulse",
          style: {
            "line-opacity": 1,
            "line-style": "dashed",
            "line-dash-pattern": [6, 6],
            "line-dash-offset": "data(pulse_offset)"
          }
        },
        {
          selector: "node[power_state = \"near\"]",
          style: {
            "border-color": "#E4C14A",
            "border-width": 3
          }
        },
        {
          selector: "node[power_state = \"over\"]",
          style: {
            "border-color": "#E05555",
            "border-width": 3
          }
        },
        {
          selector: ".warn-pulse",
          style: {
            "border-width": 4
          }
        },
        {
          selector: ".selected",
          style: {
            "border-width": 4,
            "border-color": "#F2D7A7"
          }
        },
        {
          selector: ".upstream",
          style: {
            "opacity": 1
          }
        },
        {
          selector: ".downstream",
          style: {
            "opacity": 1
          }
        },
        {
          selector: ".path",
          style: {
            "opacity": 1
          }
        },
        {
          selector: ".dimmed",
          style: {
            "opacity": 0.2
          }
        }
      ],
      layout: {
        name: "breadthfirst",
        directed: true,
        padding: 60,
        spacingFactor: 1.2,
        nodeDimensionsIncludeLabels: true
      },
      minZoom: 0.2,
      maxZoom: 2
    })
  }, [])

  useEffect(() => {
    const handler = (event: MouseEvent) => {
      const section = graphSectionRef.current
      if (!section) return
      const target = event.target as Node | null
      const path = typeof event.composedPath === "function" ? event.composedPath() : []
      const insideSection =
        (target && section.contains(target)) || (path.length > 0 && path.includes(section))
      if (insideSection) {
        event.preventDefault()
        event.stopPropagation()
      }
    }
    document.addEventListener("contextmenu", handler, { capture: true })
    window.addEventListener("contextmenu", handler, { capture: true })
    return () => {
      document.removeEventListener("contextmenu", handler, { capture: true })
      window.removeEventListener("contextmenu", handler, { capture: true })
    }
  }, [])

  const formatMachineMultiplier = (value?: number) => {
    if (value === undefined || Number.isNaN(value)) return "?"
    if (Number.isInteger(value)) return String(value)
    const fixed2 = value.toFixed(2)
    if (fixed2.endsWith("0")) return value.toFixed(1)
    return fixed2
  }
  const formatRateValue = (value: number) =>
    Number.isInteger(value) ? String(value) : value.toFixed(1)

  const formatEnergy = (value?: number) => {
    if (value === undefined || Number.isNaN(value)) return "?"
    if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}G`
    if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`
    if (value >= 1_000) return `${(value / 1_000).toFixed(1)}k`
    return Math.round(value).toString()
  }

  const formatFluidName = (name?: string, id?: string) => {
    if (name && !name.startsWith("fluid:")) return name
    const source = id || name
    if (!source) return "Unknown fluid"
    const trimmed = source.startsWith("fluid:") ? source.slice("fluid:".length) : source
    return trimmed.replace(/[_-]/g, " ").replace(/\b\w/g, char => char.toUpperCase())
  }

  const formatTargetName = (target: Target) => {
    if (target.type === "fluid") return formatFluidName(target.name, target.id)
    return target.name || target.id
  }

  const getRecipeEnergy = (recipe: RecipeOption) => recipe.duration_ticks * recipe.eut
  const getRatePerS = (value: number, unit: "min" | "sec") => (unit === "min" ? value / 60 : value)
  const formatRateNumber = (value: number, decimals: number) => {
    const fixed = value.toFixed(decimals)
    return fixed.replace(/\.0+$/, "").replace(/(\.\d*[1-9])0+$/, "$1")
  }
  const formatItemRate = (ratePerS: number) =>
    rateUnit === "min"
      ? `${formatRateNumber(ratePerS * 60, 2)} /min`
      : `${formatRateNumber(ratePerS, 2)} /s`
  const formatFluidRate = (ratePerS: number) => {
    const liters = rateUnit === "min" ? ratePerS * 60 : ratePerS
    if (liters >= 1000) {
      const kLiters = liters / 1000
      const unit = rateUnit === "min" ? "kL/min" : "kL/s"
      const decimals = kLiters >= 10 ? 1 : 2
      return `${formatRateNumber(kLiters, decimals)} ${unit}`
    }
    const unit = rateUnit === "min" ? "L/min" : "L/s"
    const decimals = liters >= 10 ? 1 : 2
    return `${formatRateNumber(liters, decimals)} ${unit}`
  }
  const getTargetForNode = (node: any): Target | null => {
    if (!node) return null
    if (node.type === "item") {
      if (!node.item_id) return null
      return {
        type: "item",
        id: node.item_id,
        meta: Number.isFinite(node.meta) ? node.meta : 0,
        name: node.label || node.item_id
      }
    }
    if (node.type === "fluid" || node.type === "gas") {
      if (!node.fluid_id) return null
      return {
        type: "fluid",
        id: node.fluid_id,
        meta: 0,
        name: formatFluidName(node.label, node.fluid_id)
      }
    }
    return null
  }
  const getInputRateForNodeId = (nodeId: string) => {
    if (!graph) return null
    const total = graph.edges
      .filter(edge => edge.source === nodeId && edge.kind === "consumes")
      .reduce((sum, edge) => sum + edge.rate_per_s, 0)
    return total > 0 ? total : null
  }

  const getFooterSegments = () => ({
    left: outputTarget ? `${formatTargetName(outputTarget)} Line` : "No output selected",
    middle: `${formatRateValue(rateUnit === "min" ? rateValue : rateValue * 60)}/min`,
    right: totalEnergyPerTick !== null ? `${formatEnergy(totalEnergyPerTick)} EU/t` : "EU/t"
  })

  const tierIndex = (tier?: string) => (tier ? TIERS.indexOf(tier) : -1)
  const getTierColor = (tier?: string) => (tier && TIER_COLORS[tier] ? TIER_COLORS[tier] : "#1C2F3F")
  const getTierCap = (tier?: string) => (tier && TIER_CAPS[tier] ? TIER_CAPS[tier] : undefined)
  const getPowerState = (eut?: number, tier?: string) => {
    if (!eut || !tier) return "ok"
    const cap = getTierCap(tier)
    if (!cap) return "ok"
    if (eut > cap) return "over"
    if (eut >= cap * 0.9) return "near"
    return "ok"
  }
  const getMaterialState = (node: any) => {
    if (node.type === "gas") return "gas"
    if (node.type === "fluid") return "fluid"
    return "solid"
  }
  const getUserVoltageIndex = () => (userVoltageTier === "Any" ? TIERS.length - 1 : tierIndex(userVoltageTier))
  const getMaxCoilTemp = () => COIL_TYPES.find(coil => coil.id === maxCoilType)?.maxTemp ?? Infinity
  const isEbfRecipe = (recipe: RecipeOption) =>
    recipe.machine_id === "gt.recipe.blastfurnace" ||
    (recipe.machine_name || "").toLowerCase().includes("blast furnace")
  const getDefaultMachineTier = (recipes: RecipeOption[]) => {
    const tierIndices = recipes
      .map(recipe => tierIndex(recipe.min_tier))
      .filter(index => index >= 0)
    if (tierIndices.length === 0) return TIERS[TIERS.length - 1]
    const maxRecipeIndex = Math.max(...tierIndices)
    const userIndex = getUserVoltageIndex()
    return TIERS[Math.min(maxRecipeIndex, userIndex)]
  }
  const getSelectedMachineTier = () => {
    if (!selectedMachineId) return undefined
    const selected = machineTierSelections[selectedMachineId] || getDefaultMachineTier(outputRecipes)
    const selectedIndex = tierIndex(selected)
    const userIndex = getUserVoltageIndex()
    if (selectedIndex > userIndex) return TIERS[userIndex]
    return selected
  }
  const isRecipeAllowed = (recipe: RecipeOption, selectedTier?: string) => {
    const minIndex = tierIndex(recipe.min_tier)
    const selectedIndex = tierIndex(selectedTier)
    const userIndex = getUserVoltageIndex()
    const voltageOk =
      minIndex < 0 || selectedIndex < 0 ? true : minIndex <= selectedIndex && minIndex <= userIndex
    if (!voltageOk) return false
    if (isEbfRecipe(recipe) && recipe.ebf_temp && Number.isFinite(recipe.ebf_temp)) {
      return recipe.ebf_temp <= getMaxCoilTemp()
    }
    return true
  }
  const getHiddenRecipeCount = (recipes: RecipeOption[], selectedTier?: string) =>
    recipes.reduce((count, recipe) => count + (isRecipeAllowed(recipe, selectedTier) ? 0 : 1), 0)
  const getOverclockTiers = (minTier?: string, selectedTier?: string) => {
    const minIndex = tierIndex(minTier)
    const selectedIndex = tierIndex(selectedTier)
    if (minIndex < 0 || selectedIndex < 0) return 0
    return Math.max(0, selectedIndex - minIndex)
  }
  const getTierOptions = (minTier?: string, maxTier?: string) => {
    const minIndex = tierIndex(minTier)
    const maxIndex = maxTier && maxTier !== "Any" ? tierIndex(maxTier) : TIERS.length - 1
    const start = minIndex >= 0 ? minIndex : 0
    const end = maxIndex >= 0 ? maxIndex : TIERS.length - 1
    if (start > end) return []
    return TIERS.slice(start, end + 1)
  }
  const getTierForRid = (rid?: string, fallback?: string) => {
    if (!rid) return "?"
    return (
      recipeTierOverrides[rid] ||
      (selectedRecipe?.rid === rid ? selectedRecipe.min_tier : undefined) ||
      fallback ||
      "?"
    )
  }

  const matchesRecipeQuery = (recipe: RecipeOption, query: string) => {
    const trimmed = query.trim().toLowerCase()
    if (!trimmed) return true
    const values = [
      recipe.machine_name,
      recipe.machine_id,
      recipe.rid,
      ...(recipe.item_inputs ?? []).flatMap(item => [item.name, item.item_id]),
      ...(recipe.fluid_inputs ?? []).flatMap(fluid => [fluid.name, fluid.fluid_id]),
      ...(recipe.item_outputs ?? []).flatMap(item => [item.name, item.item_id]),
      ...(recipe.fluid_outputs ?? []).flatMap(fluid => [fluid.name, fluid.fluid_id])
    ].filter(Boolean) as string[]
    return values.some(value => value.toLowerCase().includes(trimmed))
  }

  useEffect(() => {
    if (!outputTarget || !selectedRecipe) return
    runGraph()
  }, [outputTarget, selectedRecipe])

  useEffect(() => {
    if (!outputTarget || !selectedRecipe) return
    if (isRestoringConfigRef.current) return
    if (restoreVersion === 0) return
    runGraph()
  }, [restoreVersion])

  const scheduleGraphRun = (delay = 80) => {
    if (!outputTarget || !selectedRecipe) return
    if (graphRunTimerRef.current) {
      window.clearTimeout(graphRunTimerRef.current)
    }
    graphRunTimerRef.current = window.setTimeout(() => {
      graphRunTimerRef.current = null
      runGraph()
    }, delay)
  }

  useEffect(() => {
    if (!outputTarget || !selectedRecipe) return
    if (isRestoringConfigRef.current) return
    if (isScalingInputsRef.current) {
      isScalingInputsRef.current = false
      scheduleGraphRun(0)
      return
    }
    scheduleGraphRun()
  }, [inputTargets])

  useEffect(() => {
    if (!outputTarget || !selectedRecipe) return
    if (isRestoringConfigRef.current) return
    const nextRatePerS = getRatePerS(rateValue, rateUnit)
    const prevRatePerS = lastTargetRatePerSRef.current
    if (prevRatePerS > 0 && nextRatePerS !== prevRatePerS && inputTargets.length > 0) {
      const scale = nextRatePerS / prevRatePerS
      isScalingInputsRef.current = true
      setInputTargets(prev =>
        prev.map(entry => ({
          ...entry,
          rate_per_s: entry.rate_per_s * scale
        }))
      )
    } else {
      runGraph()
    }
    lastTargetRatePerSRef.current = nextRatePerS
  }, [rateValue, rateUnit])

  useEffect(() => {
    if (!outputTarget || !selectedRecipe) return
    if (isRestoringConfigRef.current) return
    scheduleGraphRun()
  }, [recipeOverclockTiers])

  useEffect(() => {
    if (!cyRef.current || !graph) return
    const cy = cyRef.current
    cy.elements().remove()

    const nodeMap = new Map(graph.nodes.map(node => [node.id, node]))
    const targetNodeIds = new Set<string>()
    if (outputKey) {
      targetNodeIds.add(outputKey)
    }
    inputTargets.forEach(entry => targetNodeIds.add(entry.key))
    const edgesByTarget = new Map<string, typeof graph.edges>()
    graph.edges.forEach(edge => {
      const list = edgesByTarget.get(edge.target) || []
      list.push(edge)
      edgesByTarget.set(edge.target, list)
    })
    const activeEdgeIds = new Set<string>()
    const visitedNodes = new Set<string>()
    const visitUpstream = (nodeId: string) => {
      if (visitedNodes.has(nodeId)) return
      visitedNodes.add(nodeId)
      const incomingEdges = edgesByTarget.get(nodeId) || []
      for (const edge of incomingEdges) {
        if (edge.kind === "byproduct") continue
        activeEdgeIds.add(edge.id)
        visitUpstream(edge.source)
      }
    }
    targetNodeIds.forEach(nodeId => visitUpstream(nodeId))

    const nodeFlows = new Map<string, { produced: number; consumed: number }>()
    graph.edges.forEach(edge => {
      if (edge.kind === "consumes") {
        const flow = nodeFlows.get(edge.source) || { produced: 0, consumed: 0 }
        flow.consumed += edge.rate_per_s
        nodeFlows.set(edge.source, flow)
      } else {
        const flow = nodeFlows.get(edge.target) || { produced: 0, consumed: 0 }
        flow.produced += edge.rate_per_s
        nodeFlows.set(edge.target, flow)
      }
    })
    const recipeNameCounts = graph.nodes.reduce<Record<string, number>>((acc, node) => {
      if (node.type !== "recipe") return acc
      const name = node.machine_name || node.machine_id || node.label
      acc[name] = (acc[name] || 0) + 1
      return acc
    }, {})
    const formatRecipeLabel = (node: any) => {
      const name = node.machine_name || node.machine_id || node.label
      const machines = formatMachineMultiplier(node.machines_required)
      const tier = getTierForRid(node.rid, node.min_tier)
      const base = `${machines}x ${name}\n${tier}`
      if (recipeNameCounts[name] > 1) {
        const ridSuffix = (node.rid || "").split(":").pop() || node.rid || "????"
        return `${base}\n${ridSuffix.slice(-4).toUpperCase()}`
      }
      return base
    }

    const getEdgeWidth = (ratePerS: number, isFluid: boolean) => {
      const ratePerMin = ratePerS * 60
      const normalized = isFluid ? ratePerMin / 1000 : ratePerMin
      if (normalized <= 0.25) return 1
      if (normalized <= 1) return 2.5
      if (normalized <= 4) return 4
      if (normalized <= 16) return 6
      return 6
    }

    const elements = [
      ...graph.nodes.map(node => {
        const isGas = node.type === "fluid" && GAS_FLUID_IDS.has(node.fluid_id || "")
        const nodeType = node.type === "fluid" && isGas ? "gas" : node.type
        const label =
          nodeType === "recipe"
            ? formatRecipeLabel(node)
            : nodeType === "fluid" || nodeType === "gas"
              ? formatFluidName(node.label, node.fluid_id)
              : node.label
        const tier = nodeType === "recipe" ? getTierForRid(node.rid, node.min_tier) : undefined
        return {
          data: {
            ...node,
            type: nodeType,
            label,
            tier,
            tier_color: nodeType === "recipe" ? getTierColor(tier) : undefined,
            power_state: nodeType === "recipe" ? getPowerState(node.eut, tier) : "ok"
          }
        }
      }),
      ...graph.edges.map(edge => ({
        data: {
          ...edge,
          material_state: (() => {
            const source = nodeMap.get(edge.source)
            const target = nodeMap.get(edge.target)
            const sourceGas = source?.type === "fluid" && GAS_FLUID_IDS.has(source?.fluid_id || "")
            const targetGas = target?.type === "fluid" && GAS_FLUID_IDS.has(target?.fluid_id || "")
            if (sourceGas || targetGas) return "gas"
            if (source?.type === "fluid" || target?.type === "fluid") return "fluid"
            return "solid"
          })(),
          label: (() => {
            const source = nodeMap.get(edge.source)
            const target = nodeMap.get(edge.target)
            const isFluid = source?.type === "fluid" || target?.type === "fluid"
            return isFluid ? formatFluidRate(edge.rate_per_s) : formatItemRate(edge.rate_per_s)
          })(),
          pulse_offset: 0,
          edge_width: (() => {
            const source = nodeMap.get(edge.source)
            const target = nodeMap.get(edge.target)
            const isFluid = source?.type === "fluid" || target?.type === "fluid"
            const width = getEdgeWidth(edge.rate_per_s, isFluid)
            return showFlowAnimation ? Math.min(width, 3.5) : width
          })(),
          active: activeEdgeIds.has(edge.id) ? "true" : "false",
          bottleneck: (() => {
            if (edge.kind !== "consumes") return "false"
            const flow = nodeFlows.get(edge.source)
            if (!flow) return "false"
            return flow.produced + 1e-6 < flow.consumed ? "true" : "false"
          })(),
          bottleneck_reason: (() => {
            if (edge.kind !== "consumes") return undefined
            const flow = nodeFlows.get(edge.source)
            if (!flow) return undefined
            if (flow.produced + 1e-6 < flow.consumed) {
              const sourceLabel = nodeMap.get(edge.source)?.label || "upstream"
              return `Output capped by ${sourceLabel}`
            }
            return undefined
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
      padding: 60,
      spacingFactor: 1.2,
      nodeDimensionsIncludeLabels: true,
      roots
    })
    layout.run()
    cy.nodes().forEach(node => {
      const pos = node.position()
      node.position({ x: pos.y, y: pos.x })
    })
    const incomingMap = new Map<string, Set<string>>()
    const outgoingMap = new Map<string, Set<string>>()
    graph.nodes.forEach(node => {
      incomingMap.set(node.id, new Set())
      outgoingMap.set(node.id, new Set())
    })
    graph.edges.forEach(edge => {
      incomingMap.get(edge.target)?.add(edge.source)
      outgoingMap.get(edge.source)?.add(edge.target)
    })
    const indegree = new Map<string, number>()
    const layer = new Map<string, number>()
    const queue: string[] = []
    graph.nodes.forEach(node => {
      const count = incomingMap.get(node.id)?.size ?? 0
      indegree.set(node.id, count)
      layer.set(node.id, 0)
      if (count === 0) {
        queue.push(node.id)
      }
    })
    while (queue.length > 0) {
      const id = queue.shift() as string
      const base = layer.get(id) ?? 0
      const neighbors = outgoingMap.get(id)
      if (!neighbors) continue
      neighbors.forEach(next => {
        const nextLayer = Math.max(layer.get(next) ?? 0, base + 1)
        layer.set(next, nextLayer)
        const nextIndegree = (indegree.get(next) ?? 0) - 1
        indegree.set(next, nextIndegree)
        if (nextIndegree <= 0) {
          queue.push(next)
        }
      })
    }
    const spacing = 160
    cy.nodes().forEach(node => {
      const lvl = layer.get(node.id()) ?? 0
      const pos = node.position()
      node.position({ x: lvl * spacing, y: pos.y })
    })
    const minX = Math.min(...cy.nodes().map(node => node.position().x))
    cy.nodes().forEach(node => {
      const pos = node.position()
      node.position({ x: pos.x - minX + 40, y: pos.y })
    })
    cy.fit(undefined, 30)
    setHoverInfo(null)
  }, [graph, recipeTierOverrides, selectedRecipe, rateUnit, outputKey, inputTargets, showFlowAnimation])

  useEffect(() => {
    if (!cyRef.current) return
    const cy = cyRef.current

    const showTooltip = (event: cytoscape.EventObject, lines: string[]) => {
      const rendered = event.renderedPosition || event.position
      setHoverInfo({
        x: rendered.x + 12,
        y: rendered.y + 12,
        lines
      })
    }

    const clearTooltip = () => setHoverInfo(null)

    cy.on("mouseover", "node[type = \"recipe\"]", event => {
      const data = event.target.data()
      const baseTicks = data.base_duration_ticks ?? data.duration_ticks
      const effectiveTicks = data.duration_ticks ?? baseTicks
      const baseSeconds = baseTicks ? baseTicks / 20 : null
      const effectiveSeconds = effectiveTicks ? effectiveTicks / 20 : null
      const speedMult = baseTicks && effectiveTicks ? baseTicks / effectiveTicks : null
      const lines = [
        `Base duration: ${baseSeconds ? baseSeconds.toFixed(2) : "?"}s`,
        `Speed mult: ${speedMult ? speedMult.toFixed(2) : "?"}x`,
        `Parallels: ${data.parallel ?? "?"}`,
        `Effective duration: ${effectiveSeconds ? effectiveSeconds.toFixed(2) : "?"}s`,
        `EU/t: ${data.eut ?? "?"}`
      ]
      showTooltip(event, lines)
    })
    cy.on("mouseout", "node[type = \"recipe\"]", clearTooltip)
    cy.on("mouseover", "edge[bottleneck = \"true\"]", event => {
      const reason = event.target.data("bottleneck_reason") || "Output capped"
      showTooltip(event, [reason])
    })
    cy.on("mouseout", "edge[bottleneck = \"true\"]", clearTooltip)

    const clearSelection = () => {
      cy.elements().removeClass("selected upstream downstream dimmed path")
    }
    cy.on("tap", event => {
      if (event.target === cy) {
        clearSelection()
        setRadialMenu(null)
      }
    })
    cy.on("tap", "node", event => {
      clearSelection()
      setRadialMenu(null)
      const node = event.target
      node.addClass("selected")
      const upstream = node.predecessors()
      const downstream = node.successors()
      const keep = upstream.union(downstream).union(node)
      const dim = cy.elements().difference(keep)
      dim.addClass("dimmed")
      upstream.addClass("upstream")
      downstream.addClass("downstream")
      keep.addClass("path")
    })
    cy.on("cxttap", "node", event => {
      const node = event.target
      const originalEvent = event.originalEvent as MouseEvent | undefined
      if (originalEvent?.preventDefault) {
        originalEvent.preventDefault()
      }
      if (originalEvent?.stopPropagation) {
        originalEvent.stopPropagation()
      }
      const data = node.data()
      if (!data) return
      const rect = containerRef.current?.getBoundingClientRect()
      const fallbackPos = event.renderedPosition || event.position
      const x = rect && originalEvent ? originalEvent.clientX - rect.left : fallbackPos.x
      const y = rect && originalEvent ? originalEvent.clientY - rect.top : fallbackPos.y
      if (data.type === "recipe" && data.rid) {
        setRadialMenu({
          kind: "recipe",
          x,
          y,
          rid: data.rid,
          minTier: data.min_tier
        })
        return
      }
      if (!["item", "fluid", "gas"].includes(data.type)) return
      const target = getTargetForNode(data)
      if (!target) return
      const key = getTargetKey(target)
      if (outputKey && key === outputKey) {
        setRadialMenu({
          kind: "target",
          x,
          y,
          target,
          isOutput: true,
          ratePerS: null
        })
      } else {
        const rate = getInputRateForNodeId(node.id())
        setRadialMenu({
          kind: "target",
          x,
          y,
          target,
          isOutput: false,
          ratePerS: rate
        })
      }
    })

    return () => {
      cy.removeAllListeners()
    }
  }, [graph, outputKey])

  useEffect(() => {
    if (!cyRef.current || !graph) return
    const cy = cyRef.current
    const edges = cy.edges('[active = "true"]')
    const nodes = cy.nodes('[power_state = "over"]')
    let dashOffset = 0
    let edgeTimer: number | null = null
    if (showFlowAnimation) {
      edges.addClass("pulse")
      edgeTimer = window.setInterval(() => {
        if (edges.length === 0) return
        dashOffset = (dashOffset - 2 + 1000) % 1000
        cy.batch(() => {
          edges.data("pulse_offset", dashOffset)
        })
      }, 120)
    } else {
      edges.removeClass("pulse")
      edges.data("pulse_offset", 0)
    }
    const nodeTimer = setInterval(() => {
      nodes.toggleClass("warn-pulse")
    }, 1000)
    return () => {
      if (edgeTimer) {
        clearInterval(edgeTimer)
      }
      clearInterval(nodeTimer)
    }
  }, [graph, showFlowAnimation])

  useEffect(() => {
    const key = "gtnh_saved_configs_v1"
    try {
      window.localStorage.setItem(key, JSON.stringify(savedConfigs))
    } catch (err) {
      console.warn("Failed to save configs", err)
    }
  }, [savedConfigs])

  useEffect(() => {
    const key = "gtnh_recent_recipes_v1"
    try {
      window.localStorage.setItem(key, JSON.stringify(recentRecipes))
    } catch (err) {
      console.warn("Failed to save recent recipes", err)
    }
  }, [recentRecipes])

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

  const totalEnergyPerTick = useMemo(() => {
    if (!graph) return null
    return graph.nodes.reduce((sum, node) => {
      if (node.type !== "recipe") return sum
      if (node.eut === undefined || node.machines_required === undefined) return sum
      return sum + node.eut * node.machines_required
    }, 0)
  }, [graph])

  const restoreConfig = (entry: SavedConfigEntry) => {
    const cfg = entry.config
    isRestoringConfigRef.current = true
    setOutputTarget(cfg.outputTarget)
    setSelectedRecipe(cfg.selectedRecipe)
    setInputTargets(cfg.inputTargets)
    setRateValue(cfg.rateValue)
    setRateUnit(cfg.rateUnit)
    setUserVoltageTier(cfg.userVoltageTier)
    setMaxCoilType(cfg.maxCoilType)
    setRecipeTierOverrides(cfg.recipeTierOverrides)
    setRecipeOverclockTiers(cfg.recipeOverclockTiers)
    setInputRecipeOverrides(cfg.inputRecipeOverrides)
    setMachineTierSelections(cfg.machineTierSelections)
    setShowOutputModal(false)
    setSelectedOutput(null)
    setSelectedMachineId(null)
    setOutputRecipes([])
    setMachinesForOutput([])
    setSelectionMode("output")
    setConfigTab("outputs")
    setGraphTab("graph")
    lastTargetRatePerSRef.current = getRatePerS(cfg.rateValue, cfg.rateUnit)
    setGraph(null)
    setError(null)
    window.setTimeout(() => {
      isRestoringConfigRef.current = false
      setRestoreVersion(prev => prev + 1)
    }, 0)
  }

  const saveCurrentConfig = () => {
    if (!outputTarget || !selectedRecipe) {
      setError("Select an output before saving a configuration.")
      return
    }
    const name = configName.trim() || `${formatTargetName(outputTarget)} Line`
    const id = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    const config: SavedConfig = {
      outputTarget,
      selectedRecipe,
      inputTargets,
      rateValue,
      rateUnit,
      userVoltageTier,
      maxCoilType,
      recipeTierOverrides,
      recipeOverclockTiers,
      inputRecipeOverrides,
      machineTierSelections
    }
    setSavedConfigs(prev => [
      { id, name, savedAt: new Date().toISOString(), config },
      ...prev
    ])
    setConfigName("")
  }

  const deleteConfig = (id: string) => {
    setSavedConfigs(prev => prev.filter(entry => entry.id !== id))
  }

  const buildSvgWithFooter = () => {
    if (!cyRef.current) return null
    const rawSvg = cyRef.current.svg({
      full: true,
      scale: 1,
      bg: "#161311"
    })
    const parser = new DOMParser()
    const doc = parser.parseFromString(rawSvg, "image/svg+xml")
    const svg = doc.documentElement as SVGSVGElement
    const parseSize = (value: string | null) => (value ? Number.parseFloat(value) : 0)
    let width = parseSize(svg.getAttribute("width"))
    let height = parseSize(svg.getAttribute("height"))
    const viewBox = svg.getAttribute("viewBox")
    let viewBoxParts: number[] | null = null
    if (viewBox) {
      const parts = viewBox.split(/\s+/).map(part => Number.parseFloat(part))
      if (parts.length === 4 && parts.every(Number.isFinite)) {
        viewBoxParts = parts
        width = width || parts[2]
        height = height || parts[3]
      }
    }
    if (!width || !height) {
      width = width || cyRef.current.width()
      height = height || cyRef.current.height()
    }
    const footerHeight = 56
    const nextHeight = height + footerHeight
    svg.setAttribute("width", `${width}`)
    svg.setAttribute("height", `${nextHeight}`)
    if (viewBoxParts) {
      const [minX, minY, vbWidth, vbHeight] = viewBoxParts
      svg.setAttribute("viewBox", `${minX} ${minY} ${vbWidth} ${vbHeight + footerHeight}`)
    } else {
      svg.setAttribute("viewBox", `0 0 ${width} ${nextHeight}`)
    }

    const footer = getFooterSegments()
    const ns = "http://www.w3.org/2000/svg"
    const footerGroup = doc.createElementNS(ns, "g")
    footerGroup.setAttribute("data-role", "footer")
    footerGroup.setAttribute("transform", `translate(0 ${height})`)

    const footerRect = doc.createElementNS(ns, "rect")
    footerRect.setAttribute("x", "0")
    footerRect.setAttribute("y", "0")
    footerRect.setAttribute("width", `${width}`)
    footerRect.setAttribute("height", `${footerHeight}`)
    footerRect.setAttribute("fill", "#211b16")
    footerGroup.appendChild(footerRect)

    const makeFooterText = (value: string, x: number, anchor: "start" | "middle" | "end", color: string) => {
      const text = doc.createElementNS(ns, "text")
      text.textContent = value
      text.setAttribute("x", `${x}`)
      text.setAttribute("y", `${footerHeight / 2}`)
      text.setAttribute("fill", color)
      text.setAttribute("font-family", "'Space Grotesk', sans-serif")
      text.setAttribute("font-size", "14")
      text.setAttribute("font-weight", "600")
      text.setAttribute("dominant-baseline", "middle")
      text.setAttribute("text-anchor", anchor)
      footerGroup.appendChild(text)
    }

    makeFooterText(footer.left, 18, "start", "#f4efe6")
    makeFooterText(footer.middle, width / 2, "middle", "#bcae9a")
    makeFooterText(footer.right, width - 18, "end", "#bcae9a")
    svg.appendChild(footerGroup)

    const svgText = new XMLSerializer().serializeToString(svg)
    return { svgText, width, height: nextHeight }
  }

  const renderGraphWithFooter = async () => {
    if (!cyRef.current) return null
    const pngData = cyRef.current.png({
      full: true,
      scale: 2,
      bg: "#161311"
    })
    const image = new Image()
    const footerHeight = 56
    const footer = getFooterSegments()
    return new Promise<HTMLCanvasElement>((resolve, reject) => {
      image.onload = () => {
        const canvas = document.createElement("canvas")
        canvas.width = image.width
        canvas.height = image.height + footerHeight
        const ctx = canvas.getContext("2d")
        if (!ctx) {
          reject(new Error("Canvas not supported"))
          return
        }
        ctx.drawImage(image, 0, 0)
        ctx.fillStyle = "#211b16"
        ctx.fillRect(0, image.height, canvas.width, footerHeight)
        ctx.fillStyle = "#bcae9a"
        ctx.font = "600 14px 'Space Grotesk', sans-serif"
        ctx.textBaseline = "middle"
        ctx.fillStyle = "#f4efe6"
        ctx.textAlign = "left"
        ctx.fillText(footer.left, 18, image.height + footerHeight / 2)
        ctx.fillStyle = "#bcae9a"
        ctx.textAlign = "center"
        ctx.fillText(footer.middle, canvas.width / 2, image.height + footerHeight / 2)
        ctx.textAlign = "right"
        ctx.fillText(footer.right, canvas.width - 18, image.height + footerHeight / 2)
        resolve(canvas)
      }
      image.onerror = () => reject(new Error("Failed to render image"))
      image.src = pngData
    })
  }

  const downloadBlob = (blob: Blob, filename: string) => {
    const url = URL.createObjectURL(blob)
    const link = document.createElement("a")
    link.href = url
    link.download = filename
    link.click()
    URL.revokeObjectURL(url)
  }

  const copyGraphImage = async () => {
    try {
      const canvas = await renderGraphWithFooter()
      if (!canvas) return
      const blob = await new Promise<Blob | null>(resolve => canvas.toBlob(resolve, "image/png"))
      if (!blob) return
      if (navigator.clipboard && "write" in navigator.clipboard) {
        const item = new ClipboardItem({ "image/png": blob })
        await navigator.clipboard.write([item])
      } else {
        downloadBlob(blob, "gtnh-graph.png")
      }
    } catch (err) {
      setError("Failed to copy graph image.")
    }
  }

  const downloadSvg = async () => {
    try {
      const svgPayload = buildSvgWithFooter()
      if (!svgPayload) return
      const blob = new Blob([svgPayload.svgText], { type: "image/svg+xml" })
      downloadBlob(blob, "gtnh-graph.svg")
    } catch (err) {
      setError("Failed to export SVG.")
    }
  }

  const downloadPdf = async () => {
    try {
      const svgPayload = buildSvgWithFooter()
      if (!svgPayload) return
      const wrapper = document.createElement("div")
      wrapper.style.position = "fixed"
      wrapper.style.left = "-99999px"
      wrapper.style.top = "-99999px"
      wrapper.style.width = "0"
      wrapper.style.height = "0"
      wrapper.innerHTML = svgPayload.svgText
      const svgElement = wrapper.querySelector("svg")
      if (!svgElement) {
        throw new Error("SVG export failed")
      }
      document.body.appendChild(wrapper)
      try {
        const pdf = new jsPDF({
          unit: "pt",
          format: [svgPayload.width, svgPayload.height]
        })
        await svg2pdf(svgElement, pdf, {
          xOffset: 0,
          yOffset: 0,
          scale: 1
        })
        const pdfBlob = pdf.output("blob")
        downloadBlob(pdfBlob, "gtnh-graph.pdf")
      } finally {
        wrapper.remove()
      }
    } catch (err) {
      setError("Failed to export PDF.")
    }
  }

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
    setInputTargets([])
    setInputRecipeOverrides({})
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
            name: formatFluidName(fluid.name, fluid.fluid_id)
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

  useEffect(() => {
    setRecipeQuery("")
  }, [selectedMachineId])

  useEffect(() => {
    if (!selectedMachineId || outputRecipes.length === 0) return
    setMachineTierSelections(prev => {
      if (prev[selectedMachineId]) return prev
      const defaultTier = getDefaultMachineTier(outputRecipes)
      return { ...prev, [selectedMachineId]: defaultTier }
    })
  }, [selectedMachineId, outputRecipes])

  const applyOutputSelection = (nextTarget: Target, recipe: RecipeOption) => {
    const machineTier = getSelectedMachineTier()
    const selectedTier = machineTier || recipe.min_tier
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
    setInputTargets([])
    setInputRecipeOverrides({})
    lastTargetRatePerSRef.current = getRatePerS(rateValue, rateUnit)
    setGraph(null)
    setError(null)
    setShowOutputModal(false)
    setSelectionMode("output")
    setConfigTab("outputs")
    setGraphTab("graph")
    setRecentRecipes(prev => {
      const key = `${getTargetKey(nextTarget)}:${recipe.rid}`
      const filtered = prev.filter(
        entry => `${getTargetKey(entry.target)}:${entry.recipe.rid}` !== key
      )
      return [{ target: { ...nextTarget }, recipe: { ...recipe } }, ...filtered].slice(0, 5)
    })
  }

  const getInputRateForTarget = (target: Target, recipeRid?: string, recipeOutputKey?: string) => {
    if (!graph || !recipeRid || !recipeOutputKey) return null
    const recipeNodeId = getRecipeNodeId(recipeRid, recipeOutputKey)
    const sourceId =
      target.type === "item" ? `item:${target.id}:${target.meta}` : `fluid:${target.id}`
    const edge = graph.edges.find(
      candidate =>
        candidate.kind === "consumes" &&
        candidate.target === recipeNodeId &&
        candidate.source === sourceId
    )
    return edge?.rate_per_s ?? null
  }

  const openInputSelector = (target: Target, ratePerS: number | null) => {
    if (ratePerS === null) {
      setError("Build the graph before adding inputs.")
      return
    }
    setRadialMenu(null)
    setSelectionMode("input")
    setPendingInputRate(ratePerS)
    setSelectedOutput(target)
    setSelectedMachineId(null)
    setOutputRecipes([])
    setMachinesForOutput([])
    setShowOutputModal(true)
  }

  const clearInputRecipe = (key: string) => {
    setInputTargets(prev =>
      prev.map(entry => (entry.key === key ? { ...entry, recipe: undefined } : entry))
    )
    setInputRecipeOverrides(prev => {
      const next = { ...prev }
      delete next[key]
      return next
    })
  }

  const applyInputSelection = (target: Target, recipe: RecipeOption) => {
    const ratePerS =
      pendingInputRate ?? getInputRateForTarget(target, selectedRecipe?.rid, outputKey || undefined)
    if (ratePerS === null) {
      setError("Build the graph before adding inputs.")
      return
    }
    const key = getTargetKey(target)
    const machineTier = getSelectedMachineTier()
    const selectedTier = machineTier || recipe.min_tier
    setInputTargets(prev => {
      const next = prev.filter(entry => entry.key !== key)
      next.push({ key, target, rate_per_s: ratePerS, recipe })
      return next
    })
    setInputRecipeOverrides(prev => ({ ...prev, [key]: recipe.rid }))
    if (selectedTier) {
      setRecipeTierOverrides(prev => ({ ...prev, [recipe.rid]: selectedTier }))
      setRecipeOverclockTiers(prev => ({
        ...prev,
        [recipe.rid]: getOverclockTiers(recipe.min_tier, selectedTier)
      }))
    }
    setShowOutputModal(false)
    setSelectionMode("output")
    setSelectedOutput(null)
    setSelectedMachineId(null)
    setOutputRecipes([])
    setMachinesForOutput([])
    setPendingInputRate(null)
    setConfigTab("inputs")
    setGraphTab("graph")
  }

  const openOutputModal = () => {
    setRadialMenu(null)
    setShowOutputModal(true)
    setSelectionMode("output")
    setSelectedOutput(null)
    setSelectedMachineId(null)
    setOutputRecipes([])
    setMachinesForOutput([])
    setOutputQuery("")
    setOutputResults([])
    setPendingInputRate(null)
  }

  const openOutputRecipeModal = (target: Target) => {
    setRadialMenu(null)
    setShowOutputModal(true)
    setSelectionMode("output")
    setSelectedOutput(target)
    setSelectedMachineId(null)
    setOutputRecipes([])
    setMachinesForOutput([])
    setOutputQuery("")
    setOutputResults([])
    setPendingInputRate(null)
  }

  async function runGraph() {
    setError(null)
    if (!outputTarget || !selectedRecipe || !outputKey) {
      setError("Select an output before building the graph")
      return
    }
    try {
      const extraTargets = inputTargets.filter(entry => entry.key !== outputKey)
      const recipeOverride = {
        [outputKey]: selectedRecipe.rid,
        ...inputRecipeOverrides
      }
      const targetRatePerS = getRatePerS(rateValue, rateUnit)
      const payload = {
        targets: [
          {
            target_type: outputTarget.type,
            target_id: outputTarget.id,
            target_meta: outputTarget.meta,
            target_rate_per_s: targetRatePerS
          },
          ...extraTargets.map(entry => ({
            target_type: entry.target.type,
            target_id: entry.target.id,
            target_meta: entry.target.meta,
            target_rate_per_s: entry.rate_per_s
          }))
        ],
        max_depth: 0,
        overclock_tiers: 0,
        parallel: 1,
        recipe_override: recipeOverride,
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
          <h1 className="hero-title">GT: New Horizons Production Line Planner</h1>
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
            <div className="graph-actions">
              <button className="ghost" onClick={copyGraphImage}>
                Copy image
              </button>
              <button className="ghost" onClick={downloadSvg}>
                Export SVG
              </button>
              <button className="ghost" onClick={downloadPdf}>
                Export PDF
              </button>
            </div>
          </div>
          <div className="panel-body">
            <div
              className={`graph-section ${graphTab === "graph" ? "" : "is-hidden"}`}
              ref={graphSectionRef}
            >
              <div className="graph" ref={containerRef} />
              {hoverInfo && (
                <div
                  className="graph-tooltip"
                  style={{ left: hoverInfo.x, top: hoverInfo.y }}
                >
                  {hoverInfo.lines.map(line => (
                    <div key={line}>{line}</div>
                  ))}
                </div>
              )}
              {radialMenu && (
                <div
                  className="radial-menu-backdrop"
                  onClick={() => setRadialMenu(null)}
                >
                  <div
                    className="radial-menu"
                    style={{ left: radialMenu.x, top: radialMenu.y }}
                    onClick={event => event.stopPropagation()}
                  >
                    {radialMenu.kind === "target" && (
                      <>
                        <button
                          className="radial-action radial-action-top"
                          onClick={() => {
                            if (radialMenu.isOutput) {
                              openOutputRecipeModal(radialMenu.target)
                            } else {
                              openInputSelector(radialMenu.target, radialMenu.ratePerS)
                            }
                          }}
                        >
                          Change recipe
                        </button>
                        {radialMenu.isOutput && (
                          <button
                            className="radial-action radial-action-bottom"
                            onClick={openOutputModal}
                          >
                            Change output
                          </button>
                        )}
                      </>
                    )}
                    {radialMenu.kind === "recipe" && (
                      <>
                        <div className="radial-action radial-action-top radial-label">
                          Machine tier
                        </div>
                        <div className="radial-action radial-action-right radial-select">
                          <select
                            value={getTierForRid(radialMenu.rid, radialMenu.minTier)}
                            onChange={e => {
                              const nextTier = e.target.value
                              setRecipeTierOverrides(prev => ({
                                ...prev,
                                [radialMenu.rid]: nextTier
                              }))
                              setRecipeOverclockTiers(prev => ({
                                ...prev,
                                [radialMenu.rid]: getOverclockTiers(radialMenu.minTier, nextTier)
                              }))
                            }}
                          >
                            {getTierOptions(radialMenu.minTier, userVoltageTier).map(tier => (
                              <option key={tier} value={tier}>
                                {tier}
                              </option>
                            ))}
                          </select>
                        </div>
                      </>
                    )}
                    <button
                      className="radial-center"
                      onClick={() => setRadialMenu(null)}
                    >
                      Cancel
                    </button>
                  </div>
                </div>
              )}
              {!graph && <p className="graph-empty">Select an output or load a saved configuration.</p>}
            </div>
            {graphTab === "graph" && (
              <div className="viz-footer">
                <span>
                  {outputTarget ? `${formatTargetName(outputTarget)} Line` : "No output selected"}
                </span>
                <span>
                  {formatRateValue(rateUnit === "min" ? rateValue : rateValue * 60)}/min
                </span>
                <span>
                  {totalEnergyPerTick !== null ? `${formatEnergy(totalEnergyPerTick)} EU/t` : "EU/t"}
                </span>
              </div>
            )}
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
                          {getTierOptions(selectedRecipe.min_tier, userVoltageTier).map(tier => (
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
            <button
              className={configTab === "saved" ? "active" : ""}
              onClick={() => setConfigTab("saved")}
            >
              Saved
            </button>
          </div>
          <div className="panel-body">
            <div className={`output-panel ${configTab === "outputs" ? "" : "is-hidden"}`}>
              {!outputTarget && <p className="empty">No output selected. Choose an output or load a saved config.</p>}
              {outputTarget && (
                <div className="output-summary">
                  <strong>{formatTargetName(outputTarget)}</strong>
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
                  {inputTargets.length > 0 && (
                    <>
                      <p className="inputs-title">Selected inputs</p>
                      <div className="input-targets">
                        {inputTargets.map(entry => (
                          <div key={entry.key} className="input-target">
                            <div className="input-target-actions">
                              <button
                                className="icon-button"
                                onClick={() => openInputSelector(entry.target, entry.rate_per_s)}
                                aria-label="Change recipe"
                                title="Change recipe"
                              >
                                <svg viewBox="0 0 24 24" aria-hidden="true">
                                  <path
                                    d="M4 20h4l10.5-10.5-4-4L4 16v4zm13.8-13.8 1.9-1.9a1 1 0 0 1 1.4 0l1.4 1.4a1 1 0 0 1 0 1.4l-1.9 1.9-4-4z"
                                    fill="currentColor"
                                  />
                                </svg>
                              </button>
                              <button
                                className="icon-button"
                                onClick={() => {
                                  setInputTargets(prev => prev.filter(item => item.key !== entry.key))
                                  setInputRecipeOverrides(prev => {
                                    const next = { ...prev }
                                    delete next[entry.key]
                                    return next
                                  })
                                }}
                                aria-label="Remove input"
                                title="Remove input"
                              >
                                <svg viewBox="0 0 24 24" aria-hidden="true">
                                  <path
                                    d="M18 6 6 18M6 6l12 12"
                                    stroke="currentColor"
                                    strokeWidth="2"
                                    strokeLinecap="round"
                                  />
                                </svg>
                              </button>
                            </div>
                            <div className="input-target-body">
                              <strong>{formatTargetName(entry.target)}</strong>
                              <small>
                                {entry.target.type === "item"
                                  ? `meta ${entry.target.meta}`
                                  : "fluid"}{" "}
                                |{" "}
                                {rateUnit === "min"
                                  ? (entry.rate_per_s * 60).toFixed(2)
                                  : entry.rate_per_s.toFixed(2)}
                                /{rateUnit === "min" ? "min" : "s"}
                              </small>
                              {entry.recipe && (
                                <>
                                  <small>
                                    {entry.recipe.machine_name || entry.recipe.machine_id} |{" "}
                                    {(entry.recipe.rid || "").split(":").pop()}
                                  </small>
                                  <button
                                    className="input-clear"
                                    onClick={() => clearInputRecipe(entry.key)}
                                  >
                                    Clear recipe
                                  </button>
                                </>
                              )}
                              {!entry.recipe && <small className="muted">No recipe selected.</small>}
                            </div>
                          </div>
                        ))}
                      </div>
                      {inputTargets
                        .filter(entry => entry.recipe)
                        .map(entry => {
                          if (!entry.recipe) return null
                          const recipeOutputKey = entry.key
                          return (
                            <div key={`${entry.key}:inputs`} className="input-subsection">
                              <p className="inputs-title">
                                Inputs for {formatTargetName(entry.target)}
                              </p>
                              {(entry.recipe.item_inputs ?? []).map(item => {
                                const target: Target = {
                                  type: "item",
                                  id: item.item_id,
                                  meta: item.meta,
                                  name: item.name || item.item_id
                                }
                                const key = getTargetKey(target)
                                const alreadyAdded = inputTargets.some(itemEntry => itemEntry.key === key)
                                const rate = getInputRateForTarget(
                                  target,
                                  entry.recipe?.rid,
                                  recipeOutputKey
                                )
                                const label = alreadyAdded ? "Change" : "Add"
                                return (
                                  <div key={`${entry.key}:${item.item_id}:${item.meta}`} className="input-row">
                                    <span>{item.name || item.item_id}</span>
                                    <small>x{item.count}</small>
                                    <button
                                      className="input-action"
                                      onClick={() => openInputSelector(target, rate)}
                                    >
                                      {label}
                                    </button>
                                  </div>
                                )
                              })}
                              {(entry.recipe.fluid_inputs ?? []).map(fluid => {
                                const target: Target = {
                                  type: "fluid",
                                  id: fluid.fluid_id,
                                  meta: 0,
                                  name: formatFluidName(fluid.name, fluid.fluid_id)
                                }
                                const key = getTargetKey(target)
                                const alreadyAdded = inputTargets.some(itemEntry => itemEntry.key === key)
                                const rate = getInputRateForTarget(
                                  target,
                                  entry.recipe?.rid,
                                  recipeOutputKey
                                )
                                const label = alreadyAdded ? "Change" : "Add"
                                return (
                                  <div key={`${entry.key}:${fluid.fluid_id}`} className="input-row">
                                    <span>{formatFluidName(fluid.name, fluid.fluid_id)}</span>
                                    <small>{fluid.mb} L</small>
                                    <button
                                      className="input-action"
                                      onClick={() => openInputSelector(target, rate)}
                                    >
                                      {label}
                                    </button>
                                  </div>
                                )
                              })}
                            </div>
                          )
                        })}
                    </>
                  )}
                  <p className="inputs-title">Inputs</p>
                  {selectedRecipe.item_inputs?.map(item => {
                    const target: Target = {
                      type: "item",
                      id: item.item_id,
                      meta: item.meta,
                      name: item.name || item.item_id
                    }
                    const key = getTargetKey(target)
                    const alreadyAdded = inputTargets.some(entry => entry.key === key)
                    const rate = getInputRateForTarget(target, selectedRecipe?.rid, outputKey || undefined)
                    const label = alreadyAdded ? "Change" : "Add"
                    return (
                      <div key={`${item.item_id}:${item.meta}`} className="input-row">
                        <span>{item.name || item.item_id}</span>
                        <small>x{item.count}</small>
                        <button
                          className="input-action"
                          onClick={() => {
                            openInputSelector(target, rate)
                          }}
                        >
                          {label}
                        </button>
                      </div>
                    )
                  })}
                  {selectedRecipe.fluid_inputs?.map(fluid => {
                    const target: Target = {
                      type: "fluid",
                      id: fluid.fluid_id,
                      meta: 0,
                      name: formatFluidName(fluid.name, fluid.fluid_id)
                    }
                    const key = getTargetKey(target)
                    const alreadyAdded = inputTargets.some(entry => entry.key === key)
                    const rate = getInputRateForTarget(target, selectedRecipe?.rid, outputKey || undefined)
                    const label = alreadyAdded ? "Change" : "Add"
                    return (
                      <div key={fluid.fluid_id} className="input-row">
                        <span>{formatFluidName(fluid.name, fluid.fluid_id)}</span>
                        <small>{fluid.mb} L</small>
                        <button
                          className="input-action"
                          onClick={() => {
                            openInputSelector(target, rate)
                          }}
                        >
                          {label}
                        </button>
                      </div>
                    )
                  })}
                </>
              )}
            </div>
            <div className={`options-panel ${configTab === "options" ? "" : "is-hidden"}`}>
              <label>
                Output rate
                <input
                  type="number"
                  min="0"
                  step="0.1"
                  value={rateValue}
                  onChange={e => setRateValue(Number(e.target.value))}
                />
              </label>
              <label>
                Rate unit
                <select
                  value={rateUnit}
                  onChange={e => {
                    const nextUnit = e.target.value as "min" | "sec"
                    if (nextUnit === rateUnit) return
                    setRateUnit(nextUnit)
                  }}
                >
                  <option value="min">per minute</option>
                  <option value="sec">per second</option>
                </select>
              </label>
              <label>
                Current voltage tier
                <select
                  value={userVoltageTier}
                  onChange={e => setUserVoltageTier(e.target.value)}
                >
                  <option value="Any">Any</option>
                  {TIERS.map(tier => (
                    <option key={tier} value={tier}>
                      {tier}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Max coil type
                <select
                  value={maxCoilType}
                  onChange={e => setMaxCoilType(e.target.value)}
                >
                  {COIL_TYPES.map(coil => (
                    <option key={coil.id} value={coil.id}>
                      {coil.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className="checkbox">
                <span>Flow animation</span>
                <input
                  type="checkbox"
                  checked={showFlowAnimation}
                  onChange={e => setShowFlowAnimation(e.target.checked)}
                />
              </label>
              <button onClick={runGraph}>Build graph</button>
            </div>
            <div className={`saved-panel ${configTab === "saved" ? "" : "is-hidden"}`}>
              <label>
                Save current configuration
                <input
                  placeholder="Name this configuration"
                  value={configName}
                  onChange={e => setConfigName(e.target.value)}
                />
              </label>
              <button onClick={saveCurrentConfig} disabled={!outputTarget || !selectedRecipe}>
                Save configuration
              </button>
              {savedConfigs.length === 0 && <p className="empty">No saved configurations yet.</p>}
              {savedConfigs.length > 0 && (
                <div className="saved-list">
                  {savedConfigs.map(entry => (
                    <div key={entry.id} className="saved-card">
                      <div>
                        <strong>{entry.name}</strong>
                        <small>{new Date(entry.savedAt).toLocaleString()}</small>
                      </div>
                      <div className="saved-actions">
                        <button className="ghost" onClick={() => restoreConfig(entry)}>
                          Load
                        </button>
                        <button className="ghost danger" onClick={() => deleteConfig(entry.id)}>
                          Delete
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </aside>
      </section>

      {showOutputModal && (
        <div className="modal-backdrop">
          <div className="modal">
            <div className="modal-header">
              <h2>{selectionMode === "output" ? "Select Output" : "Add Input"}</h2>
              {outputTarget && (
                <button className="modal-close" onClick={() => setShowOutputModal(false)}>
                  Close
                </button>
              )}
            </div>
            {!selectedOutput && (
              <div className="modal-section">
                <p className="modal-label">
                  {selectionMode === "output" ? "Search outputs" : "Search inputs"}
                </p>
                <input
                  placeholder="Search items or fluids..."
                  value={outputQuery}
                  onChange={e => setOutputQuery(e.target.value)}
                />
                {isLoadingOutputs && <p className="empty">Searching outputs...</p>}
                {selectionMode === "output" && recentRecipes.length > 0 && (
                  <div className="recent-outputs">
                    <p className="modal-label">Recent recipes</p>
                    <div className="output-results">
                      {recentRecipes.map(entry => (
                        <div
                          key={`recent:${getTargetKey(entry.target)}:${entry.recipe.rid}`}
                          className="output-result recent-recipe"
                        >
                          <div className="recent-recipe-info">
                            <span>{formatTargetName(entry.target)}</span>
                            <small>
                              {entry.target.type === "item" ? `meta ${entry.target.meta}` : "fluid"} •{" "}
                              {(entry.recipe.machine_name || entry.recipe.machine_id)} •{" "}
                              {(entry.recipe.rid || "").split(":").pop()}
                            </small>
                          </div>
                          <div className="recent-recipe-actions">
                            <button
                              className="ghost"
                              onClick={() => applyOutputSelection(entry.target, entry.recipe)}
                            >
                              Use recipe
                            </button>
                            <button
                              className="ghost"
                              onClick={() => {
                                setSelectedOutput(entry.target)
                                setSelectedMachineId(null)
                                setOutputRecipes([])
                                setMachinesForOutput([])
                              }}
                            >
                              Pick output
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
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
                      <span>{formatTargetName(result)}</span>
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
                    <strong>{formatTargetName(selectedOutput)}</strong>
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
                      <span>
                        {machine.machine_name}
                        {machine.recipe_count !== undefined && machine.recipe_count !== null
                          ? ` (${machine.recipe_count})`
                          : ""}
                      </span>
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
                    <strong>{formatTargetName(selectedOutput)}</strong>
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
                  <label className="machine-tier-select">
                    Machine tier
                    <select
                      value={getSelectedMachineTier() || ""}
                      onChange={e => {
                        const nextTier = e.target.value
                        if (!selectedMachineId) return
                        setMachineTierSelections(prev => ({ ...prev, [selectedMachineId]: nextTier }))
                      }}
                    >
                      {getTierOptions(undefined, userVoltageTier).map(tier => (
                        <option key={tier} value={tier}>
                          {tier}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
                <label className="recipe-search">
                  Filter recipes
                  <input
                    placeholder="Search inputs or outputs..."
                    value={recipeQuery}
                    onChange={e => setRecipeQuery(e.target.value)}
                  />
                </label>
                {isLoadingRecipes && <p className="empty">Loading recipes...</p>}
                <div className="output-grid">
                  {(() => {
                    const group = machineGroups.find(group => group.machine_id === selectedMachineId)
                    const selectedTier = getSelectedMachineTier()
                    const hiddenCount = getHiddenRecipeCount(group?.recipes || [], selectedTier)
                    const visibleRecipes = (group?.recipes || [])
                      .slice()
                      .filter(recipe => isRecipeAllowed(recipe, selectedTier))
                      .filter(recipe => matchesRecipeQuery(recipe, recipeQuery))
                      .sort((a, b) => getRecipeEnergy(a) - getRecipeEnergy(b))
                    return (
                      <>
                        {hiddenCount > 0 && (
                          <p className="hidden-recipes">
                            {hiddenCount} recipe{hiddenCount === 1 ? "" : "s"} hidden by tier
                          </p>
                        )}
                        {visibleRecipes.map(recipe => {
                      const energy = getRecipeEnergy(recipe)
                      const ridSuffix = recipe.rid.split(":").pop() || ""
                      const ridShort = ridSuffix.slice(-4).toUpperCase()
                      return (
                        <div key={recipe.rid} className="output-card">
                          <div className="output-card-header">
                            <div>
                              <strong>{recipe.machine_name || recipe.machine_id}</strong>
                              <div className="output-card-meta">
                                <small>RID {ridShort}</small>
                                <small>
                                  Energy <span className="energy-value">{formatEnergy(energy)} EU</span>
                                </small>
                                {isEbfRecipe(recipe) && recipe.ebf_temp && (
                                  <small>EBF Temp {recipe.ebf_temp} K</small>
                                )}
                              </div>
                            </div>
                            <div className="output-card-actions">
                              <button
                                className="output-card-select"
                                onClick={() =>
                                  selectionMode === "output"
                                    ? applyOutputSelection(selectedOutput, recipe)
                                    : applyInputSelection(selectedOutput, recipe)
                                }
                              >
                                {selectionMode === "output" ? "Select" : "Add"}
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
                                  <span>{formatFluidName(fluid.name, fluid.fluid_id)}</span>
                                  <small>{fluid.mb} L</small>
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
                                  <span>{formatFluidName(fluid.name, fluid.fluid_id)}</span>
                                  <small>{fluid.mb} L</small>
                                </div>
                              ))}
                            </div>
                          </div>
                        </div>
                      )
                    })}
                      </>
                    )
                  })()}
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
