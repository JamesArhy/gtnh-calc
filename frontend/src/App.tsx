import { useEffect, useMemo, useRef, useState } from "react"
import cytoscape from "cytoscape"
import cytoscapeSvg from "cytoscape-svg"
import cytoscapeDagre from "cytoscape-dagre"
import { jsPDF } from "jspdf"
import { svg2pdf } from "svg2pdf.js"
import type { GraphResponse, RecipeIOFluid, RecipeIOItem } from "./types"
import {
  fetchGraph,
  fetchMachinesByOutput,
  fetchRecipesByInput,
  fetchRecipesByOutput,
  searchFluids,
  searchItems
} from "./api"

cytoscape.use(cytoscapeSvg)
cytoscape.use(cytoscapeDagre)

const SEARCH_DEBOUNCE_MS = 300
const OUTPUT_SEARCH_LIMIT = 30
const TIERS = ["ULV", "LV", "MV", "HV", "EV", "IV", "LuV", "ZPM", "UV", "UHV"]
const RECIPE_RESULTS_LIMIT = 200
const BYPRODUCT_CHAIN_DEPTH = 3
const RADIAL_MENU_MARGIN = 130
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
const SVG_EXPORT_PADDING = 40
const SVG_EXPORT_SCALE = 4
const SVG_EXPORT_MITER_LIMIT = 10
const PX_TO_PT = 72 / 96
const EXPORT_MAX_EDGE_WIDTH = 4
const EXPORT_EDGE_WIDTH_SCALE = 0.3
const EXPORT_ARROW_SCALE = 0.7
const EXPORT_STYLE_OVERRIDES: cytoscape.Stylesheet[] = [
  {
    selector: "node",
    style: {
      "text-background-opacity": 0,
      "text-background-color": "transparent",
      "text-background-padding": 0,
      "text-background-shape": "rectangle"
    }
  },
  {
    selector: "node[export_padding = \"true\"]",
    style: {
      "background-opacity": 0,
      "border-opacity": 0,
      "width": 1,
      "height": 1,
      "label": ""
    }
  },
  {
    selector: "edge",
    style: {
      "width": 2,
      "text-background-opacity": 0,
      "text-background-color": "transparent",
      "text-outline-width": 0,
      "text-background-padding": 0,
      "text-background-shape": "round-rectangle",
      "text-background-opacity": 1,
      "text-background-color": "#161311",
      "overlay-opacity": 0,
      "underlay-opacity": 0,
      "shadow-opacity": 0,
      "shadow-blur": 0,
      "curve-style": "unbundled-bezier",
      "edge-distances": "node-position",
      "line-cap": "round",
      "line-join": "round",
      "arrow-scale": 0,
      "line-opacity": 1,
      "opacity": 1,
      "target-arrow-shape": "none"
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
    selector: "node[fluid_id]",
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
    selector: "node[type = \"recipe\"]",
    style: {
      "background-fill": "solid"
    }
  },
  {
    selector: "edge[active = \"true\"]",
    style: {
      "width": 2,
      "line-opacity": 0.85
    }
  },
  {
    selector: "edge[active = \"false\"]",
    style: {
      "width": 2,
      "line-opacity": 0.5
    }
  },
  {
    selector: "edge[bottleneck = \"true\"]",
    style: {
      "width": 2,
      "line-opacity": 0.9
    }
  }
]
const UTILIZATION_LOW = 0.5
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

type RateMode = "machines" | "output"

type InputTarget = {
  key: string
  target: Target
  rate_per_s: number
  recipe?: RecipeOption
}

type ByproductTarget = {
  id: string
  input: Target
  output: Target
  recipe: RecipeOption
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
  byproductTargets: ByproductTarget[]
  outputMachineCount?: number
  rateMode?: RateMode
  rateValue?: number
  machineCountOverrides?: Record<string, number>
  rateUnit?: "min" | "sec"
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
  const [outputMachineCount, setOutputMachineCount] = useState(1)
  const [rateMode, setRateMode] = useState<RateMode>("machines")
  const [rateValue, setRateValue] = useState(1)
  const [machineCountOverrides, setMachineCountOverrides] = useState<Record<string, number>>({})
  const [manualMachineCounts, setManualMachineCounts] = useState<Record<string, boolean>>({})
  const [rateUnit, setRateUnit] = useState<"min" | "sec">("min")
  const [userVoltageTier, setUserVoltageTier] = useState<string>("Any")
  const [maxCoilType, setMaxCoilType] = useState<string>("Any")
  const [recipeTierOverrides, setRecipeTierOverrides] = useState<Record<string, string>>({})
  const [recipeOverclockTiers, setRecipeOverclockTiers] = useState<Record<string, number>>({})
  const [inputTargets, setInputTargets] = useState<InputTarget[]>([])
  const [byproductTargets, setByproductTargets] = useState<ByproductTarget[]>([])
  const [inputRecipeOverrides, setInputRecipeOverrides] = useState<Record<string, string>>({})

  const [graph, setGraph] = useState<GraphResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [graphTab, setGraphTab] = useState<"graph" | "machines">("graph")
  const [configTab, setConfigTab] = useState<"outputs" | "inputs" | "options" | "saved">("outputs")
  const [isGraphFullscreen, setIsGraphFullscreen] = useState(false)

  const [showOutputModal, setShowOutputModal] = useState(false)
  const [outputQuery, setOutputQuery] = useState("")
  const [outputResults, setOutputResults] = useState<Target[]>([])
  const [selectedOutput, setSelectedOutput] = useState<Target | null>(null)
  const [outputRecipes, setOutputRecipes] = useState<RecipeOption[]>([])
  const [byproductRecipes, setByproductRecipes] = useState<RecipeOption[]>([])
  const [selectedMachineId, setSelectedMachineId] = useState<string | null>(null)
  const [isLoadingOutputs, setIsLoadingOutputs] = useState(false)
  const [isLoadingMachines, setIsLoadingMachines] = useState(false)
  const [isLoadingRecipes, setIsLoadingRecipes] = useState(false)
  const [isLoadingByproductRecipes, setIsLoadingByproductRecipes] = useState(false)
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
  const [showSecondaryOutputs, setShowSecondaryOutputs] = useState(true)
  const [radialMenu, setRadialMenu] = useState<
    | {
        kind: "target"
        x: number
        y: number
        target: Target
        isOutput: boolean
        ratePerS: number | null
        hasByproductSupply?: boolean
      }
    | {
        kind: "recipe"
        x: number
        y: number
        rid: string
        minTier?: string
        outputKey?: string
        machinesRequired?: number
        machinesDemand?: number
        targetRatePerS?: number | null
        isByproductChain?: boolean
      }
    | null
  >(null)
  const [radialMachineDraft, setRadialMachineDraft] = useState("")
  const graphRunTimerRef = useRef<number | null>(null)
  const isRestoringConfigRef = useRef(false)
  const [restoreVersion, setRestoreVersion] = useState(0)
  const [selectionMode, setSelectionMode] = useState<"output" | "input" | "byproduct">("output")
  const [pendingInputRate, setPendingInputRate] = useState<number | null>(null)
  const [pendingByproductInput, setPendingByproductInput] = useState<Target | null>(null)
  const [byproductConstraintEnabled, setByproductConstraintEnabled] = useState(false)
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

  const getByproductTargetId = (input: Target, output: Target, rid: string) =>
    `${getTargetKey(input)}->${getTargetKey(output)}:${rid}`

  const getRecipeNodeId = (rid: string, outputKeyValue: string) => `recipe:${rid}:${outputKeyValue}`

  const getTargetFromKey = (key?: string): Target | null => {
    if (!key) return null
    if (key.startsWith("item:")) {
      const raw = key.slice("item:".length)
      const lastColon = raw.lastIndexOf(":")
      if (lastColon <= 0) return null
      const id = raw.slice(0, lastColon)
      const meta = Number(raw.slice(lastColon + 1))
      if (!Number.isFinite(meta)) return null
      return { type: "item", id, meta }
    }
    if (key.startsWith("fluid:")) {
      const id = key.slice("fluid:".length)
      if (!id) return null
      return { type: "fluid", id, meta: 0 }
    }
    return null
  }

  const outputKey = outputTarget
    ? outputTarget.type === "item"
      ? `item:${outputTarget.id}:${outputTarget.meta}`
      : `fluid:${outputTarget.id}`
    : null

  const clampWholeNumber = (value: number, min: number) => {
    if (!Number.isFinite(value)) return min
    return Math.max(min, Math.floor(value))
  }
  const clampValue = (value: number, min: number, max: number) =>
    Math.min(Math.max(value, min), max)
  const clampMenuPosition = (value: number, size: number, margin: number) => {
    if (size <= margin * 2) return size / 2
    return clampValue(value, margin, size - margin)
  }
  const parseMachineCount = (rawValue: string, min: number) => {
    const parsed = Number(rawValue)
    if (!Number.isFinite(parsed)) return null
    return clampWholeNumber(parsed, min)
  }

  const radialRecipeMenu = radialMenu?.kind === "recipe" ? radialMenu : null
  const radialIsOutputRecipe =
    radialRecipeMenu?.rid === selectedRecipe?.rid && radialRecipeMenu?.outputKey === outputKey
  const radialCanEditMachines = !radialRecipeMenu?.isByproductChain
  const radialMinValue = radialIsOutputRecipe ? 1 : 0
  const radialMachineCountValue = radialRecipeMenu
    ? (() => {
        const fallback = Number.isFinite(radialRecipeMenu.machinesDemand)
          ? Math.max(0, Math.ceil(radialRecipeMenu.machinesDemand || 0))
          : clampWholeNumber(radialRecipeMenu.machinesRequired || 0, 0)
        if (radialIsOutputRecipe) return outputMachineCount
        if (!radialRecipeMenu.outputKey) return fallback
        return machineCountOverrides[radialRecipeMenu.outputKey] ?? fallback
      })()
    : 0
  const applyRadialMachineCount = (nextValue: number) => {
    if (!radialRecipeMenu) return
    if (!radialCanEditMachines) return
    if (rateMode !== "machines") {
      setRateMode("machines")
    }
    if (radialIsOutputRecipe) {
      setOutputMachineCount(nextValue)
      return
    }
    if (!radialRecipeMenu.outputKey) return
    setMachineCountOverrides(prev => ({
      ...prev,
      [radialRecipeMenu.outputKey]: nextValue
    }))
    setManualMachineCounts(prev => ({
      ...prev,
      [radialRecipeMenu.outputKey]: true
    }))
  }

  useEffect(() => {
    if (!radialRecipeMenu) {
      setRadialMachineDraft("")
      return
    }
    setRadialMachineDraft(String(radialMachineCountValue))
  }, [radialRecipeMenu, radialMachineCountValue])

  const handleRadialMachineInput = (rawValue: string) => {
    if (!radialCanEditMachines) return
    setRadialMachineDraft(rawValue)
    const nextValue = parseMachineCount(rawValue, radialMinValue)
    if (nextValue === null) return
    applyRadialMachineCount(nextValue)
  }

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
            "shape": "round-hexagon",
            "z-index-compare": "manual",
            "z-index": 1
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
            "transition-duration": 150,
            "z-index": 5
          }
        },
        {
          selector: "node[type = \"recipe\"].util-low",
          style: {
            "border-color": "#4FA3D1",
            "border-width": 4
          }
        },
        {
          selector: "node.machine-badge",
          style: {
            "background-color": "#EAD6A8",
            "background-opacity": 0.92,
            "background-fill": "solid",
            "border-color": "#6B4D33",
            "border-width": 1,
            "label": "data(label)",
            "font-family": "Space Grotesk, sans-serif",
            "font-weight": "700",
            "font-size": "9px",
            "color": "#2B1F14",
            "text-valign": "center",
            "text-halign": "center",
            "text-wrap": "none",
            "text-margin-y": 0,
            "text-background-opacity": 0,
            "text-outline-width": 0,
            "text-opacity": 1,
            "width": 34,
            "height": 16,
            "shape": "round-rectangle",
            "opacity": 0.95,
            "border-opacity": 1,
            "shadow-opacity": 0,
            "shadow-blur": 0,
            "shadow-offset-y": 0,
            "z-index": 30,
            "z-index-compare": "manual",
            "events": "no"
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
            "curve-style": "taxi",
            "taxi-direction": "horizontal",
            "taxi-radius": 8,
            "edge-distances": "node-position",
            "width": "data(edge_width)",
            "line-opacity": 0.55,
            "arrow-scale": 0.8,
            "line-cap": "round",
            "line-join": "round",
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
          selector: "edge[back_edge = \"true\"]",
          style: {
            "curve-style": "bezier",
            "control-point-step-size": 140,
            "line-style": "dashed",
            "line-dash-pattern": [8, 6]
          }
        },
        {
          selector: "edge[kind = \"byproduct\"]",
          style: {
            "curve-style": "taxi",
            "taxi-direction": "horizontal",
            "taxi-radius": 8,
            "line-style": "dashed",
            "line-dash-pattern": [6, 4],
            "line-cap": "round",
            "opacity": 1,
            "line-opacity": 0.7
          }
        },
        {
          selector: "edge[byproduct_chain = \"true\"]",
          style: {
            "curve-style": "taxi",
            "taxi-direction": "horizontal",
            "taxi-radius": 8,
            "line-style": "dotted",
            "line-dash-pattern": [2, 6],
            "width": "data(edge_width)",
            "line-opacity": 0.9,
            "line-cap": "round",
            "arrow-scale": 0.7,
            "source-arrow-shape": "none",
            "target-arrow-shape": "triangle",
            "line-color": "#6ECFF6",
            "target-arrow-color": "#6ECFF6",
            "text-margin-y": -12
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
        ,
        {
          selector: "node.machine-badge.dimmed",
          style: {
            "opacity": 1,
            "text-opacity": 1,
            "background-opacity": 1,
            "border-opacity": 1
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
      wheelSensitivity: 0.08,
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
        if (target instanceof HTMLElement) {
          const tag = target.tagName
          if (target.isContentEditable || tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") {
            return
          }
        }
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

  useEffect(() => {
    const handleFullscreenChange = () => {
      const section = graphSectionRef.current
      const isFullscreen = !!section && document.fullscreenElement === section
      setIsGraphFullscreen(isFullscreen)
      if (cyRef.current) {
        requestAnimationFrame(() => {
          cyRef.current?.resize()
        })
      }
    }

    const handleKeydown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return
      const section = graphSectionRef.current
      if (section && document.fullscreenElement === section) {
        document.exitFullscreen().catch(() => null)
      }
    }

    document.addEventListener("fullscreenchange", handleFullscreenChange)
    document.addEventListener("keydown", handleKeydown)
    return () => {
      document.removeEventListener("fullscreenchange", handleFullscreenChange)
      document.removeEventListener("keydown", handleKeydown)
    }
  }, [])

  const toggleGraphFullscreen = async () => {
    const section = graphSectionRef.current
    if (!section) return
    if (document.fullscreenElement === section) {
      await document.exitFullscreen().catch(() => null)
      return
    }
    await section.requestFullscreen().catch(() => null)
  }

  const formatMachineMultiplier = (value?: number) => {
    if (value === undefined || Number.isNaN(value)) return "?"
    if (Number.isInteger(value)) return String(value)
    const fixed2 = value.toFixed(2)
    if (fixed2.endsWith("0")) return value.toFixed(1)
    return fixed2
  }
  const formatRateValue = (value: number) =>
    Number.isInteger(value) ? String(value) : value.toFixed(1)
  const getRatePerS = (value: number, unit = rateUnit) =>
    unit === "min" ? value / 60 : value
  const convertRateValue = (value: number, from: "min" | "sec", to: "min" | "sec") => {
    if (from === to) return value
    return from === "min" ? value / 60 : value * 60
  }

  const targetLabelLookup = useMemo(() => {
    const lookup = new Map<string, string>()
    if (!graph) return lookup
    graph.nodes.forEach(node => {
      if (node.type !== "item" && node.type !== "fluid") return
      if (!node.label) return
      lookup.set(node.id, node.label)
    })
    return lookup
  }, [graph])

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
    const fallback = targetLabelLookup.get(getTargetKey(target))
    if (target.type === "fluid") {
      const name = formatFluidName(target.name, target.id)
      return fallback || name
    }
    return target.name || fallback || target.id
  }

  const getRecipeEnergy = (recipe: RecipeOption) => recipe.duration_ticks * recipe.eut
  const formatRateNumber = (value: number, decimals: number) => {
    const fixed = value.toFixed(decimals)
    return fixed.replace(/\.0+$/, "").replace(/(\.\d*[1-9])0+$/, "$1")
  }
  const formatOutputRate = (ratePerS: number | null) => {
    if (ratePerS === null || !Number.isFinite(ratePerS)) {
      return `--/${rateUnit === "min" ? "min" : "s"}`
    }
    const value = rateUnit === "min" ? ratePerS * 60 : ratePerS
    return `${formatRateValue(value)}/${rateUnit === "min" ? "min" : "s"}`
  }
  const formatUtilization = (value: number) => {
    if (!Number.isFinite(value)) return "?"
    const percent = value * 100
    const fixed = percent.toFixed(1)
    return fixed.endsWith(".0") ? `${Math.round(percent)}%` : `${fixed}%`
  }
  const getOutputRateFromMachines = () => {
    if (!outputTarget || !selectedRecipe) return null
    const output =
      outputTarget.type === "item"
        ? selectedRecipe.item_outputs?.find(
            item => item.item_id === outputTarget.id && item.meta === outputTarget.meta
          )
        : selectedRecipe.fluid_outputs?.find(fluid => fluid.fluid_id === outputTarget.id)
    if (!output) return null
    const perCycle =
      outputTarget.type === "item"
        ? (output as RecipeIOItem).count
        : (output as RecipeIOFluid).mb
    if (!perCycle || perCycle <= 0) return null
    const baseDuration =
      selectedRecipe.duration_ticks ?? selectedRecipe.base_duration_ticks ?? 0
    let duration = Math.max(1, Math.floor(baseDuration))
    const overclocks = Math.max(0, recipeOverclockTiers[selectedRecipe.rid] ?? 0)
    for (let i = 0; i < overclocks; i += 1) {
      duration = Math.max(1, Math.floor(duration / 2))
    }
    const perMachineRate = (perCycle / duration) * 20
    const machineCount = clampWholeNumber(outputMachineCount, 1)
    return perMachineRate * machineCount
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
  const formatChanceLabel = (chance?: number | null) => {
    if (chance === undefined || chance === null || Number.isNaN(chance)) return null
    const percent = chance <= 1 ? chance * 100 : chance
    return `${formatRateNumber(percent, percent < 10 ? 1 : 0)}%`
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
  const getUtilizationState = (utilization: number | null) => {
    if (utilization === null) return null
    if (utilization <= UTILIZATION_LOW) return "low"
    return null
  }

  const getFooterSegments = () => ({
    left: outputTarget ? `${formatTargetName(outputTarget)} Line` : "No output selected",
    middle: formatOutputRate(outputRatePerS),
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
    scheduleGraphRun()
  }, [inputTargets, byproductTargets])

  useEffect(() => {
    if (!outputTarget || !selectedRecipe) return
    if (isRestoringConfigRef.current) return
    if (rateMode !== "machines") return
    scheduleGraphRun()
  }, [outputMachineCount, rateMode])

  useEffect(() => {
    if (!outputTarget || !selectedRecipe) return
    if (isRestoringConfigRef.current) return
    if (rateMode !== "output") return
    scheduleGraphRun()
  }, [rateValue, rateMode])

  useEffect(() => {
    if (!outputTarget || !selectedRecipe) return
    if (isRestoringConfigRef.current) return
    scheduleGraphRun()
  }, [rateMode])

  useEffect(() => {
    if (!outputTarget || !selectedRecipe) return
    if (isRestoringConfigRef.current) return
    if (rateMode !== "machines") return
    scheduleGraphRun()
  }, [machineCountOverrides, rateMode])

  useEffect(() => {
    if (!outputTarget || !selectedRecipe) return
    if (isRestoringConfigRef.current) return
    scheduleGraphRun()
  }, [recipeOverclockTiers])

  useEffect(() => {
    if (!graph || rateMode !== "machines" || !outputKey) return
    if (isRestoringConfigRef.current) return
    const nextOverrides = { ...machineCountOverrides }
    let changed = false
    graph.nodes.forEach(node => {
      if (node.type !== "recipe") return
      const nodeOutputKey = node.output_key
      if (!nodeOutputKey || nodeOutputKey === outputKey) return
      if (manualMachineCounts[nodeOutputKey]) return
      if (!Number.isFinite(node.machines_demand)) return
      const suggested = Math.max(0, Math.ceil(node.machines_demand))
      if (nextOverrides[nodeOutputKey] !== suggested) {
        nextOverrides[nodeOutputKey] = suggested
        changed = true
      }
    })
    if (changed) {
      setMachineCountOverrides(nextOverrides)
    }
  }, [graph, rateMode, outputKey, manualMachineCounts, machineCountOverrides])

  const utilizationByRecipeId = useMemo(() => {
    if (!graph || rateMode !== "machines") return new Map<string, number>()
    const supplyByNode = new Map<string, number>()
    const demandByNode = new Map<string, number>()
    const produceCountByNode = new Map<string, number>()
    const demandCountByNode = new Map<string, number>()
    const inputsByRecipe = new Map<string, typeof graph.edges>()
    const outputsByRecipe = new Map<string, typeof graph.edges>()

    graph.edges.forEach(edge => {
      if (edge.kind === "consumes") {
        demandByNode.set(edge.source, (demandByNode.get(edge.source) ?? 0) + edge.rate_per_s)
        demandCountByNode.set(edge.source, (demandCountByNode.get(edge.source) ?? 0) + 1)
        const list = inputsByRecipe.get(edge.target) || []
        list.push(edge)
        inputsByRecipe.set(edge.target, list)
      } else {
        supplyByNode.set(edge.target, (supplyByNode.get(edge.target) ?? 0) + edge.rate_per_s)
        produceCountByNode.set(edge.target, (produceCountByNode.get(edge.target) ?? 0) + 1)
        if (edge.kind === "produces") {
          const list = outputsByRecipe.get(edge.source) || []
          list.push(edge)
          outputsByRecipe.set(edge.source, list)
        }
      }
    })

    const inputAvailabilityByNode = new Map<string, number>()
    graph.nodes.forEach(node => {
      if (node.type === "recipe") return
      const supply = supplyByNode.get(node.id) ?? 0
      const demand = demandByNode.get(node.id) ?? 0
      const hasSupply = (produceCountByNode.get(node.id) ?? 0) > 0
      if (!hasSupply) {
        inputAvailabilityByNode.set(node.id, 1)
        return
      }
      if (demand <= 0) {
        inputAvailabilityByNode.set(node.id, 1)
        return
      }
      const ratio = supply / demand
      inputAvailabilityByNode.set(node.id, Math.min(1, Math.max(0, ratio)))
    })

    const getOutputDemandRatio = (nodeId: string) => {
      const supply = supplyByNode.get(nodeId) ?? 0
      const demand = demandByNode.get(nodeId) ?? 0
      const hasDemand = (demandCountByNode.get(nodeId) ?? 0) > 0
      if (!hasDemand) return null
      if (supply <= 0) return 0
      return Math.min(1, Math.max(0, demand / supply))
    }

    const utilization = new Map<string, number>()
    graph.nodes.forEach(node => {
      if (node.type !== "recipe") return
      if (!Number.isFinite(node.machines_required) || (node.machines_required ?? 0) <= 0) {
        utilization.set(node.id, 0)
        return
      }
      const ratios: number[] = []
      const inputs = inputsByRecipe.get(node.id) || []
      inputs.forEach(edge => {
        ratios.push(inputAvailabilityByNode.get(edge.source) ?? 1)
      })
      const outputs = outputsByRecipe.get(node.id) || []
      outputs.forEach(edge => {
        const ratio = getOutputDemandRatio(edge.target)
        if (ratio !== null) {
          ratios.push(ratio)
        }
      })
      if (ratios.length === 0) {
        utilization.set(node.id, 1)
        return
      }
      utilization.set(node.id, Math.min(...ratios))
    })
    return utilization
  }, [graph, rateMode])

  useEffect(() => {
    if (!cyRef.current || !graph) return
    const cy = cyRef.current
    cy.elements().remove()

    const edgesForViz = showSecondaryOutputs
      ? graph.edges
      : graph.edges.filter(edge => edge.kind !== "byproduct")
    const connectedNodeIds = new Set<string>()
    edgesForViz.forEach(edge => {
      connectedNodeIds.add(edge.source)
      connectedNodeIds.add(edge.target)
    })
    const nodesForViz = graph.nodes.filter(
      node => node.type === "recipe" || connectedNodeIds.has(node.id)
    )
    const nodeMap = new Map(nodesForViz.map(node => [node.id, node]))
    const edgesBySource = new Map<string, typeof graph.edges>()
    const edgesByTarget = new Map<string, typeof graph.edges>()
    edgesForViz.forEach(edge => {
      const outgoing = edgesBySource.get(edge.source) || []
      outgoing.push(edge)
      edgesBySource.set(edge.source, outgoing)
      const incoming = edgesByTarget.get(edge.target) || []
      incoming.push(edge)
      edgesByTarget.set(edge.target, incoming)
    })

    const byproductChainEdgeIds = new Set<string>()
    const byproductChainNodeIds = new Set<string>()
    const byproductAnchors: string[] = []
    const anchorForNode = new Map<string, string>()
    const anchorSourceMap = new Map<string, string>()
    const chainDepth = new Map<string, number>()
    const anchorDirection = new Map<string, number>()

    edgesForViz.forEach(edge => {
      if (edge.kind !== "byproduct") return
      if (!nodeMap.has(edge.target)) return
      byproductChainEdgeIds.add(edge.id)
      byproductChainNodeIds.add(edge.target)
      if (!anchorForNode.has(edge.target)) {
        byproductAnchors.push(edge.target)
        anchorForNode.set(edge.target, edge.target)
        chainDepth.set(edge.target, 0)
      }
      anchorSourceMap.set(edge.target, edge.source)
    })

    const chainQueue = [...byproductAnchors]
    while (chainQueue.length > 0) {
      const nodeId = chainQueue.shift() as string
      const node = nodeMap.get(nodeId)
      if (!node) continue
      const baseDepth = chainDepth.get(nodeId) ?? 0
      const anchorId = anchorForNode.get(nodeId) ?? nodeId
      if (node.type === "recipe" && node.byproduct_input_key) {
        const outgoing = edgesBySource.get(nodeId) || []
        for (const edge of outgoing) {
          if (edge.kind === "consumes") continue
          byproductChainEdgeIds.add(edge.id)
          const targetId = edge.target
          if (!byproductChainNodeIds.has(targetId)) {
            byproductChainNodeIds.add(targetId)
            anchorForNode.set(targetId, anchorId)
            chainDepth.set(targetId, baseDepth + 1)
            chainQueue.push(targetId)
          }
        }
      } else {
        const outgoing = edgesBySource.get(nodeId) || []
        for (const edge of outgoing) {
          if (edge.kind !== "consumes") continue
          const targetNode = nodeMap.get(edge.target)
          if (!targetNode || targetNode.type !== "recipe") continue
          if (targetNode.byproduct_input_key !== nodeId) continue
          byproductChainEdgeIds.add(edge.id)
          if (!byproductChainNodeIds.has(edge.target)) {
            byproductChainNodeIds.add(edge.target)
            anchorForNode.set(edge.target, anchorId)
            chainDepth.set(edge.target, baseDepth + 1)
            chainQueue.push(edge.target)
          }
        }
      }
    }

    const targetNodeIds = new Set<string>()
    if (outputKey) {
      targetNodeIds.add(outputKey)
    }
    inputTargets.forEach(entry => targetNodeIds.add(entry.key))
    byproductTargets.forEach(entry => targetNodeIds.add(getTargetKey(entry.output)))
    const protectedNodeIds = new Set<string>([...targetNodeIds, ...byproductChainNodeIds])
    const collapseMaterialNodes = () => {
      const removedNodeIds = new Set<string>()
      const removedEdgeIds = new Set<string>()
      const addedEdges: any[] = []
      nodesForViz.forEach(node => {
        if (!["item", "fluid", "gas"].includes(node.type)) return
        if (protectedNodeIds.has(node.id)) return
        const inEdges = edgesByTarget.get(node.id) || []
        const outEdges = edgesBySource.get(node.id) || []
        if (inEdges.length !== 1 || outEdges.length !== 1) return
        const inEdge = inEdges[0]
        const outEdge = outEdges[0]
        if (inEdge.kind === "byproduct" || outEdge.kind === "byproduct") return
        const sourceNode = nodeMap.get(inEdge.source)
        const targetNode = nodeMap.get(outEdge.target)
        if (!sourceNode || !targetNode) return
        if (sourceNode.type !== "recipe" || targetNode.type !== "recipe") return
        const supplyRate = inEdge.rate_per_s
        const demandRate = outEdge.rate_per_s
        const materialLabel = node.label || node.item_id || node.fluid_id
        if (!materialLabel) return
        const isGas = node.type === "fluid" && GAS_FLUID_IDS.has(node.fluid_id || "")
        const supplyRatio =
          typeof demandRate === "number" && demandRate > 0
            ? Math.min(1, Math.max(0, supplyRate / demandRate))
            : 1
        addedEdges.push({
          id: `flow:${node.id}`,
          source: inEdge.source,
          target: outEdge.target,
          kind: "produces",
          rate_per_s: demandRate,
          material_label: materialLabel,
          material_state: isGas ? "gas" : node.type === "fluid" ? "fluid" : "solid",
          supply_ratio: supplyRatio
        })
        removedNodeIds.add(node.id)
        removedEdgeIds.add(inEdge.id)
        removedEdgeIds.add(outEdge.id)
      })
      const nextNodes = nodesForViz.filter(node => !removedNodeIds.has(node.id))
      const nextEdges = edgesForViz
        .filter(edge => !removedEdgeIds.has(edge.id))
        .concat(addedEdges)
      return { nodes: nextNodes, edges: nextEdges }
    }
    const { nodes: vizNodes, edges: vizEdges } = collapseMaterialNodes()
    const vizNodeMap = new Map(vizNodes.map(node => [node.id, node]))
    const vizEdgesBySource = new Map<string, typeof graph.edges>()
    const vizEdgesByTarget = new Map<string, typeof graph.edges>()
    vizEdges.forEach(edge => {
      const outgoing = vizEdgesBySource.get(edge.source) || []
      outgoing.push(edge)
      vizEdgesBySource.set(edge.source, outgoing)
      const incoming = vizEdgesByTarget.get(edge.target) || []
      incoming.push(edge)
      vizEdgesByTarget.set(edge.target, incoming)
    })

    const mainlineEdgeIds = new Set(
      vizEdges
        .filter(edge => !byproductChainEdgeIds.has(edge.id))
        .map(edge => edge.id)
    )
    const mainlineNodeIds = new Set<string>()
    vizEdges.forEach(edge => {
      if (!mainlineEdgeIds.has(edge.id)) return
      mainlineNodeIds.add(edge.source)
      mainlineNodeIds.add(edge.target)
    })
    if (outputKey) {
      mainlineNodeIds.add(outputKey)
    }
    const chainNodesByAnchor = new Map<string, Set<string>>()
    byproductChainNodeIds.forEach(nodeId => {
      const anchorId = anchorForNode.get(nodeId)
      if (!anchorId) return
      const list = chainNodesByAnchor.get(anchorId) || new Set<string>()
      list.add(nodeId)
      chainNodesByAnchor.set(anchorId, list)
    })
    byproductAnchors.forEach(anchorId => {
      const chainNodes = chainNodesByAnchor.get(anchorId) || new Set<string>()
      let feedsBack = false
      vizEdges.forEach(edge => {
        if (feedsBack) return
        if (edge.kind !== "consumes") return
        if (!chainNodes.has(edge.source)) return
        if (!mainlineNodeIds.has(edge.target)) return
        feedsBack = true
      })
      anchorDirection.set(anchorId, feedsBack ? -1 : 1)
    })
    const supplyByTarget = new Map<string, { total: number; count: number }>()
    const demandBySource = new Map<string, { total: number; count: number }>()
    vizEdges.forEach(edge => {
      if (edge.kind === "consumes") {
        const entry = demandBySource.get(edge.source) || { total: 0, count: 0 }
        entry.total += edge.rate_per_s
        entry.count += 1
        demandBySource.set(edge.source, entry)
        return
      }
      const entry = supplyByTarget.get(edge.target) || { total: 0, count: 0 }
      entry.total += edge.rate_per_s
      entry.count += 1
      supplyByTarget.set(edge.target, entry)
    })
    const activeEdgeIds = new Set<string>()
    const visitedNodes = new Set<string>()
    const visitUpstream = (nodeId: string) => {
      if (visitedNodes.has(nodeId)) return
      visitedNodes.add(nodeId)
      const incomingEdges = vizEdgesByTarget.get(nodeId) || []
      for (const edge of incomingEdges) {
        if (edge.kind === "byproduct") continue
        activeEdgeIds.add(edge.id)
        visitUpstream(edge.source)
      }
    }
    targetNodeIds.forEach(nodeId => visitUpstream(nodeId))

    const nodeFlows = new Map<string, { produced: number; consumed: number }>()
    const supplyCountByNode = new Map<string, number>()
    vizEdges.forEach(edge => {
      if (edge.kind === "consumes") {
        const flow = nodeFlows.get(edge.source) || { produced: 0, consumed: 0 }
        flow.consumed += edge.rate_per_s
        nodeFlows.set(edge.source, flow)
        supplyCountByNode.set(edge.source, supplyCountByNode.get(edge.source) ?? 0)
      } else {
        const flow = nodeFlows.get(edge.target) || { produced: 0, consumed: 0 }
        flow.produced += edge.rate_per_s
        nodeFlows.set(edge.target, flow)
        supplyCountByNode.set(edge.target, (supplyCountByNode.get(edge.target) ?? 0) + 1)
      }
    })
    const recipeNameCounts = vizNodes.reduce<Record<string, number>>((acc, node) => {
      if (node.type !== "recipe") return acc
      const name = node.machine_name || node.machine_id || node.label
      acc[name] = (acc[name] || 0) + 1
      return acc
    }, {})
    const formatRecipeLabel = (node: any, utilizationState: "low" | null) => {
      const name = node.machine_name || node.machine_id || node.label
      const tier = getTierForRid(node.rid, node.min_tier)
      let base = `${name}\n${tier}`
      if (recipeNameCounts[name] > 1) {
        const ridSuffix = (node.rid || "").split(":").pop() || node.rid || "????"
        base = `${base}\n${ridSuffix.slice(-4).toUpperCase()}`
      }
      if (utilizationState === "low") {
        return `${base}\n[LO]`
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

    const badgeNodes = vizNodes
      .filter(node => node.type === "recipe" && typeof node.machines_required === "number")
      .map(node => ({
        classes: "machine-badge",
        data: {
          id: `badge:${node.id}`,
          label: `x${formatMachineMultiplier(node.machines_required)}`,
          badge_for: node.id,
          type: "badge"
        },
        selectable: false,
        grabbable: false,
        locked: true
      }))

    const elements = [
      ...vizNodes.map(node => {
        const isGas = node.type === "fluid" && GAS_FLUID_IDS.has(node.fluid_id || "")
        const nodeType = node.type === "fluid" && isGas ? "gas" : node.type
        const utilization =
          nodeType === "recipe" && rateMode === "machines"
            ? utilizationByRecipeId.get(node.id) ?? null
            : null
        const utilizationState =
          nodeType === "recipe" && rateMode === "machines"
            ? getUtilizationState(utilization)
            : null
        const label =
          nodeType === "recipe"
            ? formatRecipeLabel(node, utilizationState)
            : nodeType === "fluid" || nodeType === "gas"
              ? formatFluidName(node.label, node.fluid_id)
              : node.label
        const tier = nodeType === "recipe" ? getTierForRid(node.rid, node.min_tier) : undefined
        return {
          classes: utilizationState ? `util-${utilizationState}` : "",
          data: {
            ...node,
            type: nodeType,
            label,
            tier,
            tier_color: nodeType === "recipe" ? getTierColor(tier) : undefined,
            power_state: nodeType === "recipe" ? getPowerState(node.eut, tier) : "ok",
            utilization
          }
        }
      }),
      ...badgeNodes,
      ...vizEdges
        .filter(edge => vizNodeMap.has(edge.source) && vizNodeMap.has(edge.target))
        .map(edge => ({
        data: {
          ...edge,
          byproduct_chain: byproductChainEdgeIds.has(edge.id) ? "true" : "false",
          material_state: (() => {
            if (edge.material_state) return edge.material_state
            const source = vizNodeMap.get(edge.source)
            const target = vizNodeMap.get(edge.target)
            const sourceGas = source?.type === "fluid" && GAS_FLUID_IDS.has(source?.fluid_id || "")
            const targetGas = target?.type === "fluid" && GAS_FLUID_IDS.has(target?.fluid_id || "")
            if (sourceGas || targetGas) return "gas"
            if (source?.type === "fluid" || target?.type === "fluid") return "fluid"
            return "solid"
          })(),
          label: (() => {
            if (edge.material_label) {
              const materialState = edge.material_state
              const isFluid = materialState === "fluid" || materialState === "gas"
              const baseLabel = isFluid
                ? formatFluidRate(edge.rate_per_s)
                : formatItemRate(edge.rate_per_s)
              const supplyRatio =
                typeof edge.supply_ratio === "number" && edge.rate_per_s > 0
                  ? edge.supply_ratio
                  : null
              const supplySuffix =
                supplyRatio !== null && supplyRatio < 0.999
                  ? ` (supply ${Math.round(supplyRatio * 100)}%)`
                  : ""
              return `${edge.material_label}\n${baseLabel}${supplySuffix}`
            }
            const source = vizNodeMap.get(edge.source)
            const target = vizNodeMap.get(edge.target)
            const isFluid =
              source?.type === "fluid" || source?.type === "gas" || target?.type === "fluid" || target?.type === "gas"
            const baseLabel = isFluid
              ? formatFluidRate(edge.rate_per_s)
              : formatItemRate(edge.rate_per_s)
            if (edge.kind !== "consumes") {
              const supply = supplyByTarget.get(edge.target)
              if (supply && supply.count > 1 && supply.total > 0) {
                const percent = Math.max(0, Math.round((edge.rate_per_s / supply.total) * 100))
                return `${baseLabel} (${percent}%)`
              }
              return baseLabel
            }
            const demand = demandBySource.get(edge.source)
            const supply = supplyByTarget.get(edge.source)
            const demandPercent =
              demand && demand.count > 1 && demand.total > 0
                ? Math.max(0, Math.round((edge.rate_per_s / demand.total) * 100))
                : null
            const hasSupply = (supplyCountByNode.get(edge.source) ?? 0) > 0
            const supplyRatio =
              hasSupply && demand && demand.total > 0
                ? Math.min(1, Math.max(0, (supply?.total ?? 0) / demand.total))
                : null
            const suffixes: string[] = []
            if (demandPercent !== null) {
              suffixes.push(`${demandPercent}% demand`)
            }
            if (supplyRatio !== null && supplyRatio < 0.999) {
              suffixes.push(`supply ${Math.round(supplyRatio * 100)}%`)
            }
            if (suffixes.length > 0) {
              return `${baseLabel} (${suffixes.join(", ")})`
            }
            return baseLabel
          })(),
          pulse_offset: 0,
          edge_width: (() => {
            const source = vizNodeMap.get(edge.source)
            const target = vizNodeMap.get(edge.target)
            const materialState = edge.material_state
            const isFluid =
              materialState === "fluid" ||
              materialState === "gas" ||
              source?.type === "fluid" ||
              source?.type === "gas" ||
              target?.type === "fluid" ||
              target?.type === "gas"
            const width = getEdgeWidth(edge.rate_per_s, isFluid)
            return showFlowAnimation ? Math.min(width, 3.5) : width
          })(),
          active: activeEdgeIds.has(edge.id) ? "true" : "false",
          bottleneck: (() => {
            if (edge.kind !== "consumes") return "false"
            if ((supplyCountByNode.get(edge.source) ?? 0) === 0) return "false"
            const flow = nodeFlows.get(edge.source)
            if (!flow) return "false"
            return flow.produced + 1e-6 < flow.consumed ? "true" : "false"
          })(),
          bottleneck_reason: (() => {
            if (edge.kind !== "consumes") return undefined
            if ((supplyCountByNode.get(edge.source) ?? 0) === 0) return undefined
            const flow = nodeFlows.get(edge.source)
            if (!flow) return undefined
            if (flow.produced + 1e-6 < flow.consumed) {
              const sourceLabel = vizNodeMap.get(edge.source)?.label || "upstream"
              return `Output capped by ${sourceLabel}`
            }
            return undefined
          })()
        }
      }))
    ]
    cy.add(elements)
    const mainlineNodes = cy.collection(Array.from(mainlineNodeIds).map(id => cy.getElementById(id)))
    const mainlineEdges = cy.collection(Array.from(mainlineEdgeIds).map(id => cy.getElementById(id)))
    const layoutElements = mainlineNodes.union(mainlineEdges)
    const layout = layoutElements.layout({
      name: "dagre",
      rankDir: "LR",
      ranker: "network-simplex",
      acyclicer: "greedy",
      nodeDimensionsIncludeLabels: true,
      padding: 60,
      rankSep: 180,
      nodeSep: 80,
      edgeSep: 20
    })
    layout.run()
    let minX = Infinity
    mainlineNodes.forEach(node => {
      const pos = node.position()
      if (pos.x < minX) minX = pos.x
    })
    if (!Number.isFinite(minX)) {
      minX = 0
    }
    mainlineNodes.forEach(node => {
      const pos = node.position()
      node.position({ x: pos.x - minX + 40, y: pos.y })
    })
    const mainlineSegments: Array<{ x1: number; y1: number; x2: number; y2: number }> = []
    mainlineEdgeIds.forEach(edgeId => {
      const edge = cy.getElementById(edgeId)
      if (edge.empty()) return
      const source = edge.source()
      const target = edge.target()
      if (source.empty() || target.empty()) return
      const sourcePos = source.position()
      const targetPos = target.position()
      mainlineSegments.push({
        x1: sourcePos.x,
        y1: sourcePos.y,
        x2: targetPos.x,
        y2: targetPos.y
      })
    })
    const occupiedSegments: Array<{ x1: number; y1: number; x2: number; y2: number }> = []
    const segmentDistance = (a: number, b: number) => Math.abs(a - b)
    const sharesEndpoint = (
      a: { x1: number; y1: number; x2: number; y2: number },
      b: { x1: number; y1: number; x2: number; y2: number }
    ) => {
      const eps = 1e-3
      const points = [
        [a.x1, a.y1],
        [a.x2, a.y2]
      ]
      const other = [
        [b.x1, b.y1],
        [b.x2, b.y2]
      ]
      return points.some(
        ([x, y]) =>
          other.some(([ox, oy]) => segmentDistance(x, ox) < eps && segmentDistance(y, oy) < eps)
      )
    }
    const orientation = (ax: number, ay: number, bx: number, by: number, cx: number, cy: number) => {
      const value = (by - ay) * (cx - bx) - (bx - ax) * (cy - by)
      if (Math.abs(value) < 1e-8) return 0
      return value > 0 ? 1 : 2
    }
    const onSegment = (ax: number, ay: number, bx: number, by: number, cx: number, cy: number) =>
      Math.min(ax, cx) <= bx + 1e-8 &&
      bx <= Math.max(ax, cx) + 1e-8 &&
      Math.min(ay, cy) <= by + 1e-8 &&
      by <= Math.max(ay, cy) + 1e-8
    const segmentsIntersect = (
      a: { x1: number; y1: number; x2: number; y2: number },
      b: { x1: number; y1: number; x2: number; y2: number }
    ) => {
      if (sharesEndpoint(a, b)) return false
      const o1 = orientation(a.x1, a.y1, a.x2, a.y2, b.x1, b.y1)
      const o2 = orientation(a.x1, a.y1, a.x2, a.y2, b.x2, b.y2)
      const o3 = orientation(b.x1, b.y1, b.x2, b.y2, a.x1, a.y1)
      const o4 = orientation(b.x1, b.y1, b.x2, b.y2, a.x2, a.y2)
      if (o1 !== o2 && o3 !== o4) return true
      if (o1 === 0 && onSegment(a.x1, a.y1, b.x1, b.y1, a.x2, a.y2)) return true
      if (o2 === 0 && onSegment(a.x1, a.y1, b.x2, b.y2, a.x2, a.y2)) return true
      if (o3 === 0 && onSegment(b.x1, b.y1, a.x1, a.y1, b.x2, b.y2)) return true
      if (o4 === 0 && onSegment(b.x1, b.y1, a.x2, a.y2, b.x2, b.y2)) return true
      return false
    }
    const anchorsBySource = new Map<string, string[]>()
    byproductAnchors.forEach(anchorId => {
      const sourceId = anchorSourceMap.get(anchorId)
      if (!sourceId) return
      const list = anchorsBySource.get(sourceId) || []
      list.push(anchorId)
      anchorsBySource.set(sourceId, list)
    })
    const fixedNodes = new Set(mainlineNodeIds)
    const verticalSpacing = 90
    const horizontalSpacing = 160
    const anchorSpacing = 140
    const chainOffsetX = 180
    const chainOffsetY = 80
    const badgeWidth = 34
    const badgeHeight = 16
    const badgeInset = 8
    const occupiedBounds: Array<{ x1: number; y1: number; x2: number; y2: number }> = []
    const expandBounds = (
      bounds: { x1: number; y1: number; x2: number; y2: number },
      pad: number
    ) => ({
      x1: bounds.x1 - pad,
      y1: bounds.y1 - pad,
      x2: bounds.x2 + pad,
      y2: bounds.y2 + pad
    })
    const boundsIntersect = (
      a: { x1: number; y1: number; x2: number; y2: number },
      b: { x1: number; y1: number; x2: number; y2: number }
    ) => !(a.x2 < b.x1 || a.x1 > b.x2 || a.y2 < b.y1 || a.y1 > b.y2)
    const intersectsAny = (
      bounds: { x1: number; y1: number; x2: number; y2: number }
    ) => occupiedBounds.some(existing => boundsIntersect(bounds, existing))

    mainlineNodes.forEach(node => {
      const bounds = node.boundingBox({ includeLabels: true })
      const nodeData = vizNodeMap.get(node.id())
      const pad = nodeData && nodeData.type !== "recipe" ? 130 : 48
      occupiedBounds.push(expandBounds(bounds, pad))
    })

    anchorsBySource.forEach((anchors, sourceId) => {
      const sourceNode = cy.getElementById(sourceId)
      if (sourceNode.empty()) return
      const sourcePos = sourceNode.position()
      const leftAnchors = anchors.filter(anchorId => (anchorDirection.get(anchorId) ?? 1) < 0)
      const rightAnchors = anchors.filter(anchorId => (anchorDirection.get(anchorId) ?? 1) >= 0)
      const placeGroup = (group: string[], direction: number) => {
        group.forEach((anchorId, anchorIndex) => {
          const centeredIndex = anchorIndex - (group.length - 1) / 2
          const baseX = sourcePos.x + direction * chainOffsetX
          const baseY =
            sourcePos.y +
            (direction < 0 ? -chainOffsetY : chainOffsetY) +
            centeredIndex * anchorSpacing
          const nodesForAnchor = Array.from(byproductChainNodeIds).filter(
            nodeId => anchorForNode.get(nodeId) === anchorId && !fixedNodes.has(nodeId)
          )
          if (nodesForAnchor.length === 0) return
          const nodesForAnchorSet = new Set(nodesForAnchor)
          const anchorEdges = vizEdges.filter(edge => {
            if (!byproductChainEdgeIds.has(edge.id)) return false
            return (
              nodesForAnchorSet.has(edge.source) ||
              nodesForAnchorSet.has(edge.target) ||
              edge.target === anchorId ||
              edge.source === anchorId
            )
          })
          const nodesByDepth = new Map<number, string[]>()
          nodesForAnchor.forEach(nodeId => {
            const depth = chainDepth.get(nodeId) ?? 0
            const list = nodesByDepth.get(depth) || []
            list.push(nodeId)
            nodesByDepth.set(depth, list)
          })
          const sortedDepths = Array.from(nodesByDepth.keys()).sort((a, b) => a - b)
          const placeNodes = (offsetX: number, offsetY: number) => {
            sortedDepths.forEach(depth => {
              const row = nodesByDepth.get(depth) || []
              const rowHeight = (row.length - 1) * verticalSpacing
              row.forEach((nodeId, index) => {
                const node = cy.getElementById(nodeId)
                if (node.empty()) return
                const offsetYLocal = row.length > 1 ? -rowHeight / 2 + index * verticalSpacing : 0
                const x = baseX + offsetX + direction * depth * horizontalSpacing
                const y = baseY + offsetY + offsetYLocal
                node.position({ x, y })
              })
            })
          }
          const getAnchorSegments = () =>
            anchorEdges
              .map(edge => {
                const edgeElement = cy.getElementById(edge.id)
                if (edgeElement.empty()) return null
                const source = edgeElement.source()
                const target = edgeElement.target()
                if (source.empty() || target.empty()) return null
                const sourcePos = source.position()
                const targetPos = target.position()
                return {
                  x1: sourcePos.x,
                  y1: sourcePos.y,
                  x2: targetPos.x,
                  y2: targetPos.y
                }
              })
              .filter((segment): segment is { x1: number; y1: number; x2: number; y2: number } =>
                Boolean(segment)
              )
          const countCrossings = (segments: Array<{ x1: number; y1: number; x2: number; y2: number }>) => {
            let crossings = 0
            segments.forEach(segment => {
              mainlineSegments.forEach(mainline => {
                if (segmentsIntersect(segment, mainline)) crossings += 1
              })
              occupiedSegments.forEach(existing => {
                if (segmentsIntersect(segment, existing)) crossings += 1
              })
            })
            return crossings
          }
          const chainNodes = nodesForAnchor
            .map(nodeId => cy.getElementById(nodeId))
            .filter(node => !node.empty())
          const chainCollection = cy.collection(chainNodes)
          const candidates = [
            { x: 0, y: 0 },
            { x: 0, y: anchorSpacing },
            { x: 0, y: -anchorSpacing },
            { x: direction * horizontalSpacing * 2, y: 0 },
            { x: direction * horizontalSpacing * 2, y: anchorSpacing },
            { x: direction * horizontalSpacing * 2, y: -anchorSpacing },
            { x: direction * horizontalSpacing * 4, y: 0 }
          ]
          let bestCandidate = candidates[0]
          let bestCrossings = Number.POSITIVE_INFINITY
          let bestOverlap = true
          candidates.forEach(candidate => {
            placeNodes(candidate.x, candidate.y)
            const bounds = chainCollection.boundingBox({ includeLabels: true })
            const padded = expandBounds(bounds, 30)
            const overlap = intersectsAny(padded)
            const segments = getAnchorSegments()
            const crossings = countCrossings(segments)
            if (
              (bestOverlap && !overlap) ||
              (overlap === bestOverlap && crossings < bestCrossings)
            ) {
              bestCandidate = candidate
              bestCrossings = crossings
              bestOverlap = overlap
            }
          })
          placeNodes(bestCandidate.x, bestCandidate.y)
          const finalBounds = chainCollection.boundingBox({ includeLabels: true })
          occupiedBounds.push(expandBounds(finalBounds, 30))
          getAnchorSegments().forEach(segment => occupiedSegments.push(segment))
        })
      }
      placeGroup(leftAnchors, -1)
      placeGroup(rightAnchors, 1)
    })
    const leafDistance = 210
    const boundsByNode = new Map<string, { x1: number; y1: number; x2: number; y2: number }>()
    vizNodes.forEach(node => {
      const element = cy.getElementById(node.id)
      if (element.empty()) return
      const pad = node.type === "recipe" ? 24 : 80
      boundsByNode.set(node.id, expandBounds(element.boundingBox({ includeLabels: true }), pad))
    })
    const intersectsOthers = (
      bounds: { x1: number; y1: number; x2: number; y2: number },
      nodeId: string
    ) => {
      for (const [otherId, otherBounds] of boundsByNode.entries()) {
        if (otherId === nodeId) continue
        if (boundsIntersect(bounds, otherBounds)) return true
      }
      return false
    }
    const adjustLeafNode = (nodeId: string) => {
      const element = cy.getElementById(nodeId)
      if (element.empty()) return
      const neighbors = vizEdges.filter(
        edge => edge.source === nodeId || edge.target === nodeId
      )
      if (neighbors.length !== 1) return
      const edge = neighbors[0]
      const neighborId = edge.source === nodeId ? edge.target : edge.source
      const neighborNode = vizNodeMap.get(neighborId)
      if (!neighborNode || neighborNode.type !== "recipe") return
      const neighborElement = cy.getElementById(neighborId)
      if (neighborElement.empty()) return
      const pos = element.position()
      const neighborPos = neighborElement.position()
      let dx = pos.x - neighborPos.x
      let dy = pos.y - neighborPos.y
      const length = Math.hypot(dx, dy) || 1
      dx /= length
      dy /= length
      const candidate = { x: neighborPos.x + dx * leafDistance, y: neighborPos.y + dy * leafDistance }
      element.position(candidate)
      let bounds = expandBounds(element.boundingBox({ includeLabels: true }), 80)
      if (intersectsOthers(bounds, nodeId)) {
        const perp = { x: -dy, y: dx }
        const nudge = 180
        const candidateA = {
          x: neighborPos.x + dx * leafDistance + perp.x * nudge,
          y: neighborPos.y + dy * leafDistance + perp.y * nudge
        }
        element.position(candidateA)
        bounds = expandBounds(element.boundingBox({ includeLabels: true }), 80)
        if (intersectsOthers(bounds, nodeId)) {
          const candidateB = {
            x: neighborPos.x + dx * leafDistance - perp.x * nudge,
            y: neighborPos.y + dy * leafDistance - perp.y * nudge
          }
          element.position(candidateB)
          bounds = expandBounds(element.boundingBox({ includeLabels: true }), 80)
          if (intersectsOthers(bounds, nodeId)) {
            element.position(pos)
            return
          }
        }
      }
      boundsByNode.set(nodeId, bounds)
    }
    vizNodes.forEach(node => {
      if (node.type === "recipe") return
      adjustLeafNode(node.id)
    })
    const positionBadgeFor = (targetId: string) => {
      const badge = cy.getElementById(`badge:${targetId}`)
      if (badge.empty()) return
      const target = cy.getElementById(targetId)
      if (target.empty()) return
      const pos = target.position()
      const width = target.width()
      const height = target.height()
      if (![pos.x, pos.y, width, height].every(Number.isFinite)) return
      badge.unlock()
      badge.position({
        x: pos.x + width / 2 - badgeWidth / 2 - badgeInset,
        y: pos.y - height / 2 + badgeHeight / 2 + badgeInset
      })
      badge.lock()
    }
    const positionBadges = () => {
      const badgeElements = cy.nodes(".machine-badge")
      if (badgeElements.empty()) return
      badgeElements.forEach(badge => {
        const targetId = badge.data("badge_for") as string | undefined
        if (!targetId) return
        positionBadgeFor(targetId)
      })
    }
    positionBadges()
    mainlineEdgeIds.forEach(edgeId => {
      const edge = cy.getElementById(edgeId)
      if (edge.empty()) return
      const source = edge.source()
      const target = edge.target()
      if (source.empty() || target.empty()) return
      const isBackEdge = source.position().x - target.position().x > 20
      edge.data("back_edge", isBackEdge ? "true" : "false")
    })
    cy.fit(undefined, 30)
    cy.off("position", "node[type = \"recipe\"]")
    cy.on("position", "node[type = \"recipe\"]", event => {
      const nodeId = event.target.id()
      if (!nodeId) return
      positionBadgeFor(nodeId)
    })
    cy.off("dragfree", "node[type = \"recipe\"]")
    cy.on("dragfree", "node[type = \"recipe\"]", event => {
      const nodeId = event.target.id()
      if (!nodeId) return
      positionBadgeFor(nodeId)
    })
    requestAnimationFrame(() => {
      if (!cyRef.current) return
      positionBadges()
    })
    setHoverInfo(null)
  }, [
    graph,
    recipeTierOverrides,
    selectedRecipe,
    rateUnit,
    rateMode,
    outputKey,
    inputTargets,
    showFlowAnimation,
    showSecondaryOutputs
  ])

  const byproductRecipeNodeIds = useMemo(() => {
    if (!graph) return new Set<string>()
    return new Set(
      graph.nodes
        .filter(node => Boolean(node.byproduct_input_key))
        .map(node => node.id)
    )
  }, [graph])

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
      if (rateMode === "machines") {
        const utilization =
          typeof data.utilization === "number" ? data.utilization : null
        lines.push(`Utilization: ${utilization === null ? "--" : formatUtilization(utilization)}`)
      }
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
      const tapped = event.target
      const tappedData = tapped.data()
      const node =
        tappedData?.type === "badge" && tappedData.badge_for
          ? cy.getElementById(tappedData.badge_for)
          : tapped
      if (node.empty()) return
      node.addClass("selected")
      const upstream = node.predecessors()
      const downstream = node.successors()
      const keep = upstream.union(downstream).union(node)
      const keepNodes = keep.nodes()
      const keepBadges = cy.nodes(".machine-badge").filter(badge => {
        const targetId = badge.data("badge_for")
        if (!targetId) return false
        return keepNodes.some(entry => entry.id() === targetId)
      })
      const keepWithBadges = keep.union(keepBadges)
      const dim = cy.elements().difference(keepWithBadges)
      dim.addClass("dimmed")
      upstream.addClass("upstream")
      downstream.addClass("downstream")
      keepWithBadges.addClass("path")
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
      let x = rect && originalEvent ? originalEvent.clientX - rect.left : fallbackPos.x
      let y = rect && originalEvent ? originalEvent.clientY - rect.top : fallbackPos.y
      if (rect) {
        x = clampMenuPosition(x, rect.width, RADIAL_MENU_MARGIN)
        y = clampMenuPosition(y, rect.height, RADIAL_MENU_MARGIN)
      }
      if (data.type === "recipe" && data.rid) {
        const outputKeyValue =
          typeof data.output_key === "string"
            ? data.output_key
            : typeof data.outputKey === "string"
              ? data.outputKey
              : typeof data.id === "string" &&
                  typeof data.rid === "string" &&
                  data.id.startsWith(`recipe:${data.rid}:`)
                ? data.id.slice(`recipe:${data.rid}:`.length)
                : undefined
        setRadialMenu({
          kind: "recipe",
          x,
          y,
          rid: data.rid,
          minTier: data.min_tier,
          outputKey: outputKeyValue,
          machinesRequired: data.machines_required,
          machinesDemand: data.machines_demand,
          targetRatePerS: data.target_rate_per_s ?? null,
          isByproductChain: Boolean(data.byproduct_input_key)
        })
        return
      }
      if (!["item", "fluid", "gas"].includes(data.type)) return
      const target = getTargetForNode(data)
      if (!target) return
      const key = getTargetKey(target)
      const hasByproductSupply = !!graph?.edges.some(edge => {
        if (edge.target !== node.id()) return false
        if (edge.kind === "byproduct") return true
        if (edge.kind === "produces") {
          return byproductRecipeNodeIds.has(edge.source)
        }
        return false
      })
      if (outputKey && key === outputKey) {
        setRadialMenu({
          kind: "target",
          x,
          y,
          target,
          isOutput: true,
          ratePerS: null,
          hasByproductSupply
        })
      } else {
        const rate = getInputRateForNodeId(node.id())
        setRadialMenu({
          kind: "target",
          x,
          y,
          target,
          isOutput: false,
          ratePerS: rate,
          hasByproductSupply
        })
      }
    })

    return () => {
      cy.removeAllListeners()
    }
  }, [graph, outputKey, rateMode, byproductRecipeNodeIds])

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
        machinesDemand: node.machines_demand,
        outputKey: node.output_key,
        minTier: node.min_tier,
        utilization: rateMode === "machines" ? utilizationByRecipeId.get(node.id) ?? null : null,
        duration_ticks: node.duration_ticks,
        eut: node.eut,
        overclock_tiers: node.overclock_tiers
      }))
  }, [graph, rateMode, utilizationByRecipeId])

  const outputRatePerS = useMemo(() => {
    if (rateMode === "output") {
      const safeValue = clampWholeNumber(rateValue, 1)
      const ratePerS = getRatePerS(safeValue)
      return Number.isFinite(ratePerS) ? ratePerS : null
    }
    if (graph && outputKey && selectedRecipe) {
      const outputNodeId = getRecipeNodeId(selectedRecipe.rid, outputKey)
      const node = graph.nodes.find(candidate => candidate.id === outputNodeId)
      return node?.target_rate_per_s ?? null
    }
    return getOutputRateFromMachines()
  }, [
    graph,
    outputKey,
    selectedRecipe,
    outputTarget,
    outputMachineCount,
    recipeOverclockTiers,
    rateMode,
    rateValue,
    rateUnit
  ])

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
    const savedOverrides = cfg.machineCountOverrides ?? {}
    setOutputTarget(cfg.outputTarget)
    setSelectedRecipe(cfg.selectedRecipe)
    setInputTargets(cfg.inputTargets)
    setByproductTargets(cfg.byproductTargets ?? [])
    setOutputMachineCount(cfg.outputMachineCount ?? 1)
    setRateMode(cfg.rateMode ?? "machines")
    setRateValue(cfg.rateValue ?? 1)
    setMachineCountOverrides(savedOverrides)
    setManualMachineCounts(
      Object.keys(savedOverrides).reduce<Record<string, boolean>>((acc, key) => {
        acc[key] = true
        return acc
      }, {})
    )
    setRateUnit(cfg.rateUnit ?? "min")
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
    setPendingByproductInput(null)
    setConfigTab("outputs")
    setGraphTab("graph")
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
      byproductTargets,
      outputMachineCount,
      rateMode,
      rateValue,
      machineCountOverrides,
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

  const ensureCanvas2SvgTextPatch = () => {
    const c2s = (window as any).C2S
    if (!c2s || c2s.__gtnhTextPatch) return
    const proto = c2s.prototype
    if (!proto || typeof proto.__applyText !== "function") return
    const originalApplyText = proto.__applyText
    proto.__applyText = function (text: string, x: number, y: number, type: string) {
      const prevElement = this.__currentElement
      const prevPath = this.__currentDefaultPath
      const prevPosition = this.__currentPosition
        ? { ...this.__currentPosition }
        : this.__currentPosition
      originalApplyText.call(this, text, x, y, type)
      this.__currentElement = prevElement
      this.__currentDefaultPath = prevPath
      this.__currentPosition = prevPosition
    }
    c2s.__gtnhTextPatch = true
  }

  const addSvgPaddingNodes = (cy: cytoscape.Core, padding: number) => {
    const elements = cy.elements()
    if (elements.length === 0 || padding <= 0) return () => undefined
    const bbox = elements.boundingBox({ includeLabels: true, includeOverlays: true })
    if (
      !Number.isFinite(bbox.x1) ||
      !Number.isFinite(bbox.y1) ||
      !Number.isFinite(bbox.x2) ||
      !Number.isFinite(bbox.y2)
    ) {
      return () => undefined
    }
    const idBase = `export-padding-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`
    const nodes = cy.add([
      {
        group: "nodes",
        data: { id: `${idBase}-tl`, export_padding: "true" },
        position: { x: bbox.x1 - padding, y: bbox.y1 - padding }
      },
      {
        group: "nodes",
        data: { id: `${idBase}-br`, export_padding: "true" },
        position: { x: bbox.x2 + padding, y: bbox.y2 + padding }
      }
    ])
    return () => {
      cy.remove(nodes)
    }
  }

  const buildSvgPayload = () => {
    if (!cyRef.current) return null
    ensureCanvas2SvgTextPatch()
    const sourceCy = cyRef.current
    const baseStyle = sourceCy.style().json()
    const exportStyle = [...baseStyle, ...EXPORT_STYLE_OVERRIDES]
    const sourceElements = sourceCy.elements()
    const sourceBounds = sourceElements.boundingBox({ includeLabels: true, includeOverlays: true })
    if (
      !Number.isFinite(sourceBounds.x1) ||
      !Number.isFinite(sourceBounds.y1) ||
      !Number.isFinite(sourceBounds.x2) ||
      !Number.isFinite(sourceBounds.y2)
    ) {
      return null
    }
    const width = Math.max(1, Math.ceil(sourceBounds.w + SVG_EXPORT_PADDING * 2))
    const height = Math.max(1, Math.ceil(sourceBounds.h + SVG_EXPORT_PADDING * 2))
    const dx = SVG_EXPORT_PADDING - sourceBounds.x1
    const dy = SVG_EXPORT_PADDING - sourceBounds.y1
    const normalizeEdgeWidth = (value: unknown) => {
      const width = typeof value === "number" && Number.isFinite(value) ? value : 2
      return Math.max(1, Math.min(EXPORT_MAX_EDGE_WIDTH, width * EXPORT_EDGE_WIDTH_SCALE))
    }
    const preserveNodeIds = new Set<string>()
    if (outputKey) {
      preserveNodeIds.add(outputKey)
    }
    inputTargets.forEach(entry => preserveNodeIds.add(entry.key))
    byproductTargets.forEach(entry => {
      preserveNodeIds.add(getTargetKey(entry.input))
      preserveNodeIds.add(getTargetKey(entry.output))
    })
    const simplifySvgElements = (elements: cytoscape.ElementDefinition[]) => {
      const nodes = elements.filter(element => element.group === "nodes")
      const edges = elements.filter(element => element.group === "edges")
      const nodeById = new Map<string, cytoscape.ElementDefinition>()
      nodes.forEach(node => {
        const nodeId = (node.data as any)?.id as string | undefined
        if (nodeId) {
          nodeById.set(nodeId, node)
        }
      })
      const incoming = new Map<string, cytoscape.ElementDefinition[]>()
      const outgoing = new Map<string, cytoscape.ElementDefinition[]>()
      edges.forEach(edge => {
        const data = edge.data as any
        if (!data) return
        const source = data.source as string | undefined
        const target = data.target as string | undefined
        if (!source || !target) return
        const outList = outgoing.get(source) || []
        outList.push(edge)
        outgoing.set(source, outList)
        const inList = incoming.get(target) || []
        inList.push(edge)
        incoming.set(target, inList)
      })
      const removedNodeIds = new Set<string>()
      const removedEdgeIds = new Set<string>()
      const addedEdges: cytoscape.ElementDefinition[] = []
      const getEdgeData = (edge: cytoscape.ElementDefinition) => edge.data as any
      nodes.forEach(node => {
        const data = node.data as any
        if (!data) return
        const nodeId = data.id as string | undefined
        if (!nodeId) return
        if (preserveNodeIds.has(nodeId)) return
        const nodeType = data.type as string | undefined
        if (!nodeType || !["item", "fluid", "gas"].includes(nodeType)) return
        const inEdges = incoming.get(nodeId) || []
        const outEdges = outgoing.get(nodeId) || []
        if (inEdges.length !== 1 || outEdges.length !== 1) return
        const inEdge = inEdges[0]
        const outEdge = outEdges[0]
        const inData = getEdgeData(inEdge)
        const outData = getEdgeData(outEdge)
        const sourceId = inData?.source as string | undefined
        const targetId = outData?.target as string | undefined
        if (!sourceId || !targetId || sourceId === targetId) return
        const sourceNode = nodeById.get(sourceId)
        const targetNode = nodeById.get(targetId)
        if (!sourceNode || !targetNode) return
        const sourceType = (sourceNode.data as any)?.type
        const targetType = (targetNode.data as any)?.type
        if (sourceType !== "recipe" || targetType !== "recipe") return
        const rate = typeof outData?.rate_per_s === "number" ? outData.rate_per_s : inData?.rate_per_s
        const isFluid = nodeType === "fluid" || nodeType === "gas"
        const materialName = data.label || data.item_id || data.fluid_id || "Material"
        const rateLabel =
          typeof rate === "number"
            ? isFluid
              ? formatFluidRate(rate)
              : formatItemRate(rate)
            : null
        const label = rateLabel ? `${materialName}\n${rateLabel}` : materialName
        const edgeWidth = Math.max(
          Number(inData?.edge_width ?? 2),
          Number(outData?.edge_width ?? 2)
        )
        const kind =
          inData?.kind === "byproduct" || outData?.kind === "byproduct" ? "byproduct" : "flow"
        const materialState = nodeType === "gas" ? "gas" : isFluid ? "fluid" : "solid"
        addedEdges.push({
          group: "edges",
          data: {
            id: `flow:${nodeId}`,
            source: sourceId,
            target: targetId,
            kind,
            material_state: materialState,
            label,
            rate_per_s: rate,
            edge_width: edgeWidth,
            active: inData?.active ?? outData?.active ?? "true"
          }
        })
        removedNodeIds.add(nodeId)
        if (inData?.id) removedEdgeIds.add(inData.id)
        if (outData?.id) removedEdgeIds.add(outData.id)
      })
      const nextNodes = nodes.filter(node => {
        const nodeId = (node.data as any)?.id as string | undefined
        return !nodeId || !removedNodeIds.has(nodeId)
      })
      const nextEdges = edges.filter(edge => {
        const edgeId = (edge.data as any)?.id as string | undefined
        return !edgeId || !removedEdgeIds.has(edgeId)
      })
      return [...nextNodes, ...nextEdges, ...addedEdges]
    }
    const elements = simplifySvgElements(
      sourceElements.jsons().map(element => {
      if (element.group === "edges") {
        return {
          ...element,
          data: {
            ...(element.data || {}),
            edge_width: normalizeEdgeWidth(element.data?.edge_width)
          }
        }
      }
      if (element.group !== "nodes") return element
      const position = element.position
      if (
        position &&
        Number.isFinite(position.x) &&
        Number.isFinite(position.y)
      ) {
        return {
          ...element,
          position: { x: position.x + dx, y: position.y + dy }
        }
      }
      return {
        ...element,
        position: { x: dx, y: dy }
      }
    })
    )
    const container = document.createElement("div")
    container.style.position = "fixed"
    container.style.left = "-10000px"
    container.style.top = "-10000px"
    container.style.width = `${width}px`
    container.style.height = `${height}px`
    container.style.pointerEvents = "none"
    document.body.appendChild(container)
    const cy = cytoscape({
      container,
      elements,
      style: exportStyle,
      layout: { name: "preset" },
      zoom: 1,
      pan: { x: 0, y: 0 },
      userZoomingEnabled: false,
      userPanningEnabled: false
    })
    try {
      cy.resize()
      const rawSvg = cy.svg({
        full: true,
        scale: SVG_EXPORT_SCALE,
        bg: "#161311"
      })
      const parser = new DOMParser()
      const doc = parser.parseFromString(rawSvg, "image/svg+xml")
      const svg = doc.documentElement as SVGSVGElement
      const scaledWidth = width * SVG_EXPORT_SCALE
      const scaledHeight = height * SVG_EXPORT_SCALE
      svg.setAttribute("shape-rendering", "geometricPrecision")
      svg.setAttribute("width", `${width}`)
      svg.setAttribute("height", `${height}`)
      svg.setAttribute("viewBox", `0 0 ${scaledWidth} ${scaledHeight}`)
      svg.querySelectorAll("path, polyline, polygon, line").forEach(path => {
        path.setAttribute("stroke-linejoin", "round")
        path.setAttribute("stroke-linecap", "round")
        path.setAttribute("stroke-miterlimit", String(SVG_EXPORT_MITER_LIMIT))
      })
      const svgText = new XMLSerializer().serializeToString(svg)
      return { svgText, width, height }
    } finally {
      cy.destroy()
      container.remove()
    }
  }

  const renderGraphCanvas = async ({
    padding = 0,
    scale = 2
  }: { padding?: number; scale?: number } = {}) => {
    if (!cyRef.current) return null
    const pngData = cyRef.current.png({
      full: true,
      scale,
      bg: "#161311"
    })
    const image = new Image()
    return new Promise<HTMLCanvasElement>((resolve, reject) => {
      image.onload = () => {
        const canvas = document.createElement("canvas")
        canvas.width = image.width + padding * 2
        canvas.height = image.height + padding * 2
        const ctx = canvas.getContext("2d")
        if (!ctx) {
          reject(new Error("Canvas not supported"))
          return
        }
        ctx.fillStyle = "#161311"
        ctx.fillRect(0, 0, canvas.width, canvas.height)
        ctx.drawImage(image, padding, padding)
        resolve(canvas)
      }
      image.onerror = () => reject(new Error("Failed to render image"))
      image.src = pngData
    })
  }

  const renderGraphWithFooter = async () => {
    const canvas = await renderGraphCanvas()
    if (!canvas) return null
    const footerHeight = 56
    const footer = getFooterSegments()
    const next = document.createElement("canvas")
    next.width = canvas.width
    next.height = canvas.height + footerHeight
    const nextCtx = next.getContext("2d")
    if (!nextCtx) throw new Error("Canvas not supported")
    nextCtx.drawImage(canvas, 0, 0)
    nextCtx.fillStyle = "#211b16"
    nextCtx.fillRect(0, canvas.height, next.width, footerHeight)
    nextCtx.font = "600 14px 'Space Grotesk', sans-serif"
    nextCtx.textBaseline = "middle"
    nextCtx.fillStyle = "#f4efe6"
    nextCtx.textAlign = "left"
    nextCtx.fillText(footer.left, 18, canvas.height + footerHeight / 2)
    nextCtx.fillStyle = "#bcae9a"
    nextCtx.textAlign = "center"
    nextCtx.fillText(footer.middle, next.width / 2, canvas.height + footerHeight / 2)
    nextCtx.textAlign = "right"
    nextCtx.fillText(footer.right, next.width - 18, canvas.height + footerHeight / 2)
    return next
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
      const svgPayload = buildSvgPayload()
      if (!svgPayload) return
      const blob = new Blob([svgPayload.svgText], { type: "image/svg+xml" })
      downloadBlob(blob, "gtnh-graph.svg")
    } catch (err) {
      setError("Failed to export SVG.")
    }
  }

  const downloadPdf = async () => {
    try {
      const svgPayload = buildSvgPayload()
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
      const pdf = new jsPDF({
        unit: "pt",
        format: [svgPayload.width * PX_TO_PT, svgPayload.height * PX_TO_PT]
      })
      try {
        await svg2pdf(svgElement, pdf, {
          xOffset: 0,
          yOffset: 0,
          scale: PX_TO_PT
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

  const recipeProducesTarget = (recipe: RecipeOption, target: Target | null) => {
    if (!target) return false
    if (target.type === "item") {
      return (recipe.item_outputs ?? []).some(
        item => item.item_id === target.id && item.meta === target.meta
      )
    }
    return (recipe.fluid_outputs ?? []).some(fluid => fluid.fluid_id === target.id)
  }

  const byproductRecipeIds = useMemo(() => {
    return new Set(byproductRecipes.map(recipe => recipe.rid))
  }, [byproductRecipes])

  const byproductOutputSuggestions = useMemo(() => {
    const outputs = new Map<string, Target>()
    for (const recipe of byproductRecipes) {
      for (const item of recipe.item_outputs ?? []) {
        const target: Target = {
          type: "item",
          id: item.item_id,
          meta: item.meta,
          name: item.name || item.item_id
        }
        const key = getTargetKey(target)
        if (!outputs.has(key)) {
          outputs.set(key, target)
        }
      }
      for (const fluid of recipe.fluid_outputs ?? []) {
        const target: Target = {
          type: "fluid",
          id: fluid.fluid_id,
          meta: 0,
          name: fluid.name || fluid.fluid_id
        }
        const key = getTargetKey(target)
        if (!outputs.has(key)) {
          outputs.set(key, target)
        }
      }
    }
    return Array.from(outputs.values())
  }, [byproductRecipes])

  const byproductRecipesForSelectedOutput = useMemo(() => {
    if (selectionMode !== "byproduct" || !selectedOutput) return []
    return byproductRecipes.filter(recipe => recipeProducesTarget(recipe, selectedOutput))
  }, [byproductRecipes, selectionMode, selectedOutput])

  const byproductMachineIdsForSelectedOutput = useMemo(() => {
    const machineIds = new Set<string>()
    for (const recipe of byproductRecipesForSelectedOutput) {
      machineIds.add(recipe.machine_id)
    }
    return machineIds
  }, [byproductRecipesForSelectedOutput])

  const machinesForSelection = useMemo(() => {
    if (selectionMode !== "byproduct") return machinesForOutput
    if (!selectedOutput) return []
    return machinesForOutput.filter(machine => byproductMachineIdsForSelectedOutput.has(machine.machine_id))
  }, [machinesForOutput, byproductMachineIdsForSelectedOutput, selectionMode, selectedOutput])

  useEffect(() => {
    if (outputTarget) return
    setSelectedOutput(null)
    setSelectedMachineId(null)
    setOutputRecipes([])
    setMachinesForOutput([])
    setInputTargets([])
    setByproductTargets([])
    setInputRecipeOverrides({})
    setPendingByproductInput(null)
  }, [outputTarget])

  useEffect(() => {
    if (!outputTarget) {
      setByproductConstraintEnabled(false)
    }
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
    if (selectionMode !== "byproduct" || !pendingByproductInput || !showOutputModal) {
      setByproductRecipes([])
      setIsLoadingByproductRecipes(false)
      return
    }
    const input = pendingByproductInput
    const useDownstreamFilter = byproductConstraintEnabled && !!outputTarget
    setIsLoadingByproductRecipes(true)
    fetchRecipesByInput({
      input_type: input.type,
      item_id: input.type === "item" ? input.id : undefined,
      meta: input.type === "item" ? input.meta : undefined,
      fluid_id: input.type === "fluid" ? input.id : undefined,
      limit: RECIPE_RESULTS_LIMIT,
      downstream_type: useDownstreamFilter ? outputTarget?.type : undefined,
      downstream_item_id:
        useDownstreamFilter && outputTarget?.type === "item" ? outputTarget.id : undefined,
      downstream_meta:
        useDownstreamFilter && outputTarget?.type === "item" ? outputTarget.meta : undefined,
      downstream_fluid_id:
        useDownstreamFilter && outputTarget?.type === "fluid" ? outputTarget.id : undefined,
      max_depth: useDownstreamFilter ? BYPRODUCT_CHAIN_DEPTH : undefined
    })
      .then(data => setByproductRecipes(data.recipes || []))
      .catch(() => setByproductRecipes([]))
      .finally(() => setIsLoadingByproductRecipes(false))
  }, [
    selectionMode,
    pendingByproductInput,
    byproductConstraintEnabled,
    outputTarget,
    showOutputModal
  ])

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
    setByproductTargets([])
    setInputRecipeOverrides({})
    setMachineCountOverrides({})
    setManualMachineCounts({})
    setGraph(null)
    setError(null)
    setShowOutputModal(false)
    setSelectionMode("output")
    setPendingByproductInput(null)
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
    setPendingByproductInput(null)
    setSelectedOutput(target)
    setSelectedMachineId(null)
    setOutputRecipes([])
    setMachinesForOutput([])
    setShowOutputModal(true)
  }

  const openByproductSelector = (target: Target) => {
    setRadialMenu(null)
    setSelectionMode("byproduct")
    setPendingByproductInput(target)
    setPendingInputRate(null)
    setSelectedOutput(null)
    setSelectedMachineId(null)
    setOutputRecipes([])
    setMachinesForOutput([])
    setOutputQuery("")
    setOutputResults([])
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
    setPendingByproductInput(null)
    setSelectedOutput(null)
    setSelectedMachineId(null)
    setOutputRecipes([])
    setMachinesForOutput([])
    setPendingInputRate(null)
    setConfigTab("inputs")
    setGraphTab("graph")
  }

  const recipeConsumesTarget = (recipe: RecipeOption, target: Target | null) => {
    if (!target) return false
    if (target.type === "item") {
      return (recipe.item_inputs ?? []).some(
        item => item.item_id === target.id && item.meta === target.meta
      )
    }
    return (recipe.fluid_inputs ?? []).some(fluid => fluid.fluid_id === target.id)
  }

  const applyByproductSelection = (output: Target, recipe: RecipeOption) => {
    if (!pendingByproductInput) {
      setError("Choose a feedback input before selecting a recipe.")
      return
    }
    if (!recipeConsumesTarget(recipe, pendingByproductInput)) {
      setError(
        `Selected recipe does not consume ${formatTargetName(pendingByproductInput)}.`
      )
      return
    }
    const id = getByproductTargetId(pendingByproductInput, output, recipe.rid)
    const machineTier = getSelectedMachineTier()
    const selectedTier = machineTier || recipe.min_tier
    setByproductTargets(prev => {
      const next = prev.filter(entry => entry.id !== id)
      next.push({ id, input: pendingByproductInput, output, recipe })
      return next
    })
    if (selectedTier) {
      setRecipeTierOverrides(prev => ({ ...prev, [recipe.rid]: selectedTier }))
      setRecipeOverclockTiers(prev => ({
        ...prev,
        [recipe.rid]: getOverclockTiers(recipe.min_tier, selectedTier)
      }))
    }
    setShowOutputModal(false)
    setSelectionMode("output")
    setPendingByproductInput(null)
    setSelectedOutput(null)
    setSelectedMachineId(null)
    setOutputRecipes([])
    setMachinesForOutput([])
    setPendingInputRate(null)
    setConfigTab("inputs")
    setGraphTab("graph")
  }

  const editByproductTarget = (entry: ByproductTarget) => {
    setByproductTargets(prev => prev.filter(item => item.id !== entry.id))
    openByproductSelector(entry.input)
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
    setPendingByproductInput(null)
  }

  const closeOutputModal = () => {
    setShowOutputModal(false)
    setSelectionMode("output")
    setPendingInputRate(null)
    setPendingByproductInput(null)
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
    setPendingByproductInput(null)
  }

  const handleRadialRecipeChange = () => {
    if (!radialRecipeMenu) return
    const targetKeyValue = radialRecipeMenu.outputKey
    const target = getTargetFromKey(targetKeyValue)
    if (!target) return
    if (outputKey && targetKeyValue === outputKey) {
      openOutputRecipeModal(target)
      return
    }
    const rate =
      radialRecipeMenu.targetRatePerS ??
      getInputRateForTarget(target, radialRecipeMenu.rid, targetKeyValue)
    openInputSelector(target, rate)
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
      const safeMachineCount = clampWholeNumber(outputMachineCount, 1)
      const outputRatePerS =
        rateMode === "output"
          ? getRatePerS(clampWholeNumber(rateValue, 1))
          : getOutputRateFromMachines() ?? 0
      const sanitizedOverrides = Object.entries(machineCountOverrides).reduce<Record<string, number>>(
        (acc, [key, value]) => {
          if (!key || key === outputKey) return acc
          acc[key] = clampWholeNumber(value, 0)
          return acc
        },
        {}
      )
      const payload = {
        targets: [
          {
            target_type: outputTarget.type,
            target_id: outputTarget.id,
            target_meta: outputTarget.meta,
            target_rate_per_s: outputRatePerS,
            target_machine_count: rateMode === "machines" ? safeMachineCount : undefined
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
        recipe_overclock_tiers: recipeOverclockTiers,
        machine_count_overrides: rateMode === "machines" ? sanitizedOverrides : {},
        byproduct_targets: byproductTargets.map(entry => ({
          input_type: entry.input.type,
          input_id: entry.input.id,
          input_meta: entry.input.meta,
          output_type: entry.output.type,
          output_id: entry.output.id,
          output_meta: entry.output.meta,
          recipe_rid: entry.recipe.rid
        }))
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
              className={`graph-section ${graphTab === "graph" ? "" : "is-hidden"}${
                radialMenu ? " menu-open" : ""
              }${isGraphFullscreen ? " is-fullscreen" : ""}`}
              ref={graphSectionRef}
            >
              <div className="graph" ref={containerRef} />
              <button
                className="graph-fullscreen-toggle"
                type="button"
                aria-pressed={isGraphFullscreen}
                onClick={toggleGraphFullscreen}
              >
                {isGraphFullscreen ? "Exit full screen" : "Full screen"}
              </button>
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
                    onPointerDown={event => event.stopPropagation()}
                    onMouseDown={event => event.stopPropagation()}
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
                        {radialMenu.hasByproductSupply && !radialMenu.isOutput && (
                          <button
                            className="radial-action radial-action-right"
                            onClick={() => openByproductSelector(radialMenu.target)}
                          >
                            Feedback loop
                          </button>
                        )}
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
                        <button
                          className="radial-action radial-action-top"
                          onClick={handleRadialRecipeChange}
                        >
                          Change recipe
                        </button>
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
                        <div className="radial-action radial-action-bottom radial-input">
                          <span>Machines</span>
                          <input
                            type="number"
                            min={radialMinValue}
                            step="1"
                            value={radialMachineDraft}
                            disabled={!radialCanEditMachines}
                            onClick={event => event.stopPropagation()}
                            onPointerDown={event => event.stopPropagation()}
                            onMouseDown={event => event.stopPropagation()}
                            onKeyDown={event => {
                              if (event.key === "ArrowUp" || event.key === "ArrowDown") {
                                event.preventDefault()
                                const delta = event.key === "ArrowUp" ? 1 : -1
                                const parsed = parseMachineCount(radialMachineDraft, radialMinValue)
                                const baseValue =
                                  parsed === null ? radialMachineCountValue : parsed
                                const nextValue = clampWholeNumber(baseValue + delta, radialMinValue)
                                setRadialMachineDraft(String(nextValue))
                                applyRadialMachineCount(nextValue)
                              }
                              event.stopPropagation()
                            }}
                            onChange={e => {
                              handleRadialMachineInput(e.currentTarget.value)
                            }}
                            onBlur={() => {
                              const parsed = parseMachineCount(radialMachineDraft, radialMinValue)
                              if (parsed === null) {
                                setRadialMachineDraft(String(radialMachineCountValue))
                              } else {
                                setRadialMachineDraft(String(parsed))
                              }
                            }}
                            onPaste={event => {
                              const text = event.clipboardData.getData("text")
                              event.preventDefault()
                              handleRadialMachineInput(text)
                            }}
                          />
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
                  {formatOutputRate(outputRatePerS)}
                </span>
                <span>
                  {totalEnergyPerTick !== null ? `${formatEnergy(totalEnergyPerTick)} EU/t` : "EU/t"}
                </span>
              </div>
            )}
            <div className={`machines-panel ${graphTab === "machines" ? "" : "is-hidden"}`}>
              {recipeStats.length === 0 && <p className="empty">No machine stats yet.</p>}
              {recipeStats.map(node => {
                const nodeOutputKey = node.outputKey
                const isOutputRecipe = nodeOutputKey && nodeOutputKey === outputKey
                const defaultCount = Number.isFinite(node.machinesDemand)
                  ? Math.max(0, Math.ceil(node.machinesDemand || 0))
                  : clampWholeNumber(node.machines || 0, 0)
                const machineCountValue = isOutputRecipe
                  ? outputMachineCount
                  : nodeOutputKey
                    ? machineCountOverrides[nodeOutputKey] ?? defaultCount
                    : defaultCount
                const canEditMachines = isOutputRecipe || nodeOutputKey
                return (
                  <div key={node.id} className="machine-card">
                    <strong>{node.label}</strong>
                    {canEditMachines ? (
                      <label className="machine-count">
                        Machines
                        <input
                          type="number"
                          min={isOutputRecipe ? "1" : "0"}
                          step="1"
                          value={machineCountValue}
                          onChange={e => {
                            const minValue = isOutputRecipe ? 1 : 0
                            const nextValue = clampWholeNumber(Number(e.target.value), minValue)
                            if (rateMode !== "machines") {
                              setRateMode("machines")
                            }
                            if (isOutputRecipe) {
                              setOutputMachineCount(nextValue)
                            } else if (nodeOutputKey) {
                              setMachineCountOverrides(prev => ({
                                ...prev,
                                [nodeOutputKey]: nextValue
                              }))
                              setManualMachineCounts(prev => ({ ...prev, [nodeOutputKey]: true }))
                            }
                          }}
                        />
                      </label>
                    ) : (
                      <span>
                        {node.machines === undefined || Number.isNaN(node.machines)
                          ? "?"
                          : Number.isInteger(node.machines)
                            ? node.machines
                            : node.machines.toFixed(1)}{" "}
                        machines
                      </span>
                    )}
                    {node.utilization !== null && node.utilization !== undefined && (
                      <span>Utilization: {formatUtilization(node.utilization)}</span>
                    )}
                    {node.rid && (
                      <div className="machine-tier">
                        <label>
                          Tier
                          <select
                            value={getTierForRid(node.rid, node.minTier)}
                            onChange={e => {
                              const nextTier = e.target.value
                              setRecipeTierOverrides(prev => ({
                                ...prev,
                                [node.rid as string]: nextTier
                              }))
                              setRecipeOverclockTiers(prev => ({
                                ...prev,
                                [node.rid as string]: getOverclockTiers(node.minTier, nextTier)
                              }))
                            }}
                          >
                            {getTierOptions(node.minTier, userVoltageTier).map(tier => (
                              <option key={tier} value={tier}>
                                {tier}
                              </option>
                            ))}
                          </select>
                        </label>
                      </div>
                    )}
                  </div>
                )
              })}
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
              {outputTarget && (
                <>
                  <label className="rate-mode-toggle">
                    Rate mode
                    <div className="rate-mode-buttons">
                      <button
                        type="button"
                        className={rateMode === "machines" ? "active" : ""}
                        onClick={() => setRateMode("machines")}
                      >
                        Machine rate
                      </button>
                      <button
                        type="button"
                        className={rateMode === "output" ? "active" : ""}
                        onClick={() => setRateMode("output")}
                      >
                        Output rate
                      </button>
                    </div>
                  </label>
                  {rateMode === "machines" && (
                    <label>
                      Output machines
                      <input
                        type="number"
                        min="1"
                        step="1"
                        value={outputMachineCount}
                        onChange={e => {
                          const nextValue = clampWholeNumber(Number(e.target.value), 1)
                          setOutputMachineCount(nextValue)
                        }}
                      />
                    </label>
                  )}
                  {rateMode === "output" && (
                    <label>
                      Output rate {rateUnit === "min" ? "(per min)" : "(per sec)"}
                      <input
                        type="number"
                        min="1"
                        step="1"
                        value={rateValue}
                        onChange={e => {
                          const nextValue = clampWholeNumber(Number(e.target.value), 1)
                          setRateValue(nextValue)
                        }}
                      />
                    </label>
                  )}
                </>
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
                  {byproductTargets.length > 0 && (
                    <>
                      <p className="inputs-title">Feedback loops</p>
                      <div className="input-targets">
                        {byproductTargets.map(entry => (
                          <div key={entry.id} className="input-target">
                            <div className="input-target-actions">
                              <button
                                className="icon-button"
                                onClick={() => editByproductTarget(entry)}
                                aria-label="Change feedback loop"
                                title="Change feedback loop"
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
                                onClick={() =>
                                  setByproductTargets(prev =>
                                    prev.filter(item => item.id !== entry.id)
                                  )
                                }
                                aria-label="Remove feedback loop"
                                title="Remove feedback loop"
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
                              <strong>
                                {formatTargetName(entry.input)}{" -> "}
                                {formatTargetName(entry.output)}
                              </strong>
                              <small>
                                {entry.recipe.machine_name || entry.recipe.machine_id}{" "}
                                | {(entry.recipe.rid || "").split(":").pop()}
                              </small>
                            </div>
                          </div>
                        ))}
                      </div>
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
                Rate display unit
                <select
                  value={rateUnit}
                  onChange={e => {
                    const nextUnit = e.target.value as "min" | "sec"
                    if (nextUnit === rateUnit) return
                    if (rateMode === "output") {
                      const converted = convertRateValue(rateValue, rateUnit, nextUnit)
                      setRateValue(clampWholeNumber(Math.round(converted), 1))
                    }
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
              <label className="checkbox">
                <span>Show secondary outputs</span>
                <input
                  type="checkbox"
                  checked={showSecondaryOutputs}
                  onChange={e => setShowSecondaryOutputs(e.target.checked)}
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
              <h2>
                {selectionMode === "output"
                  ? "Select Output"
                  : selectionMode === "byproduct"
                    ? "Add Feedback Loop"
                    : "Add Input"}
              </h2>
              <button className="modal-close" onClick={closeOutputModal}>
                Close
              </button>
            </div>
            {!selectedOutput && (
              <div className="modal-section">
                {selectionMode === "byproduct" && pendingByproductInput && (
                  <p className="modal-label">
                    Using feedback input: {formatTargetName(pendingByproductInput)}
                  </p>
                )}
                {selectionMode === "byproduct" && pendingByproductInput && (
                  <>
                    <label className="checkbox">
                      <span>
                        Only show outputs that lead to{" "}
                        {outputTarget ? formatTargetName(outputTarget) : "the selected output"}
                      </span>
                      <input
                        type="checkbox"
                        checked={byproductConstraintEnabled}
                        disabled={!outputTarget}
                        onChange={e => setByproductConstraintEnabled(e.target.checked)}
                      />
                    </label>
                    {!outputTarget && (
                      <p className="muted">
                        Select a planner output to enable downstream filtering.
                      </p>
                    )}
                    <p className="modal-label">Feedback loop outputs</p>
                    {isLoadingByproductRecipes && (
                      <p className="empty">Loading feedback loop recipes...</p>
                    )}
                    {!isLoadingByproductRecipes && byproductRecipes.length > 0 && (
                      <p className="muted">
                        {byproductRecipes.length} recipes consume this feedback input.
                      </p>
                    )}
                    <div className="output-results">
                      {byproductOutputSuggestions.map(result => (
                        <button
                          key={`byproduct:${result.type}:${result.id}:${result.meta}`}
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
                    {!isLoadingByproductRecipes && byproductOutputSuggestions.length === 0 && (
                      <p className="empty">No recipes consume this feedback input.</p>
                    )}
                  </>
                )}
                <p className="modal-label">
                  {selectionMode === "output"
                    ? "Search outputs"
                    : selectionMode === "byproduct"
                      ? "Search outputs"
                      : "Search inputs"}
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
                  {machinesForSelection.map(machine => (
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
                {!isLoadingMachines && machinesForSelection.length === 0 && (
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
                      .filter(
                        recipe =>
                          selectionMode !== "byproduct" || byproductRecipeIds.has(recipe.rid)
                      )
                      .filter(
                        recipe =>
                          selectionMode !== "byproduct" ||
                          recipeConsumesTarget(recipe, pendingByproductInput)
                      )
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
                                    : selectionMode === "byproduct"
                                      ? applyByproductSelection(selectedOutput, recipe)
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
                              {(recipe.item_outputs ?? []).map((item, index) => (
                                <div
                                  key={`${item.item_id}:${item.meta}:${item.count}:${String(item.chance)}:${index}`}
                                  className="output-io-row"
                                >
                                  <span>{item.name || item.item_id}</span>
                                  <small>
                                    x{item.count}
                                    {formatChanceLabel(item.chance)
                                      ? ` · ${formatChanceLabel(item.chance)}`
                                      : ""}
                                  </small>
                                </div>
                              ))}
                              {(recipe.fluid_outputs ?? []).map((fluid, index) => (
                                <div
                                  key={`${fluid.fluid_id}:${fluid.mb}:${String(fluid.chance)}:${index}`}
                                  className="output-io-row"
                                >
                                  <span>{formatFluidName(fluid.name, fluid.fluid_id)}</span>
                                  <small>
                                    {fluid.mb} L
                                    {formatChanceLabel(fluid.chance)
                                      ? ` · ${formatChanceLabel(fluid.chance)}`
                                      : ""}
                                  </small>
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
