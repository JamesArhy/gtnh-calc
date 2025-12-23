export type NodeType = "item" | "fluid" | "recipe"

export interface GraphNode {
  id: string
  type: NodeType
  label: string
  rid?: string
  machine_id?: string
  machine_name?: string
  machines_required?: number
  per_machine_rate_per_s?: number
  duration_ticks?: number
  eut?: number
  overclock_tiers?: number
  parallel?: number
  target_rate_per_s?: number
}

export type RecipeIOItem = {
  item_id: string
  meta: number
  count: number
  name?: string
}

export type RecipeIOFluid = {
  fluid_id: string
  mb: number
  name?: string
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
