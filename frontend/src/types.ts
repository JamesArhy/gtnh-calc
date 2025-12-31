export type NodeType = "item" | "fluid" | "recipe"

export interface GraphNode {
  id: string
  type: NodeType
  label: string
  rid?: string
  output_key?: string
  item_id?: string
  meta?: number
  fluid_id?: string
  machine_id?: string
  machine_name?: string
  machines_required?: number
  machines_demand?: number
  per_machine_rate_per_s?: number
  min_tier?: string
  base_duration_ticks?: number
  base_eut?: number
  duration_ticks?: number
  eut?: number
  overclock_tiers?: number
  parallel?: number
  target_rate_per_s?: number
  byproduct_input_key?: string
}

export type RecipeIOItem = {
  item_id: string
  meta: number
  count: number
  name?: string
  chance?: number | null
}

export type RecipeIOFluid = {
  fluid_id: string
  mb: number
  name?: string
  chance?: number | null
}

export interface GraphEdge {
  id: string
  source: string
  target: string
  kind: "consumes" | "produces" | "byproduct"
  rate_per_s: number
}

export interface GraphResponse {
  nodes: GraphNode[]
  edges: GraphEdge[]
  meta: Record<string, unknown>
}
