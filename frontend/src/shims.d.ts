declare module "cytoscape-svg" {
  const cytoscapeSvg: any
  export default cytoscapeSvg
}

declare module "svg2pdf.js" {
  export const svg2pdf: (svg: SVGSVGElement, pdf: any, options?: {
    xOffset?: number
    yOffset?: number
    scale?: number
  }) => Promise<void> | void
}
