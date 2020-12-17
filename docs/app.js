// set up SVG for D3
const colors = d3.scaleOrdinal(d3.schemeCategory10);

class Graph {
    constructor() {
        // set up initial nodes and links
        //  - nodes are known by 'id', not by index in array.
        //  - reflexive edges are indicated on the node (as a bold black circle).
        //  - links are always source < target; edge directions are set by 'left' and 'right'.
        this.nodes = [];
        this.links = [];
        this.size = 0;
    }

    addNode(x, y) {
        this.nodes.push({id: this.size++, reflexive: false, x: x, y: y});
    }

    removeNode(node) {
        this.nodes.splice(this.nodes.indexOf(node), 1);
        const toSplice = this.links.filter((l) => l.source === node || l.target === node);
        for (const l of toSplice) {
            this.links.splice(this.links.indexOf(l), 1);
        }
    }

    addLink(id1, id2) {
        // add link to graph (update if exists)
        // NB: links are strictly source.id < target.id; arrows separately specified by booleans
        const isRight = id1 < id2;
        const source = isRight ? id1 : id2;
        const target = isRight ? id2 : id1;
        const link = this.links.filter((l) => l.source === source && l.target === target)[0];
        if (link) {
            link[isRight ? 'right' : 'left'] = true;
        } else {
            this.links.push({source, target, left: !isRight, right: isRight});
            return this.links[this.links.length - 1];
        }
        return link;
    }

    removeLink(link) {
        this.links.splice(this.links.indexOf(link), 1);
    }

    adjacencyMatrix() {
        const n = this.nodes.length;
        const idToIndex = new Map();
        for (let i = 0; i < n; ++i) {
            idToIndex.set(this.nodes[i].id, i);
        }
        const adjacency_matrix = Array(n).fill().map(() => Array(n).fill(0));
        for (const link of this.links) {
            const sourceIndex = idToIndex.get(link.source.id)
            const targetIndex = idToIndex.get(link.target.id)
            if (link.right) {
                adjacency_matrix[sourceIndex][targetIndex] = 1
            }
            if (link.left) {
                adjacency_matrix[targetIndex][sourceIndex] = 1
            }
        }
        return adjacency_matrix;
    }

    draw(svg_element, height, width) {
        const self = this;

        const svg = d3.select(svg_element)
            .attr("height", height)
            .attr("width", width)
            .attr("tabindex", "-1");

        svg.on('contextmenu', () => {
            d3.event.preventDefault();
        });

        // init D3 force layout
        const force = d3.forceSimulation()
            .force('link', d3.forceLink().id((d) => d.id).distance(100))
            .force('charge', d3.forceManyBody().strength(-500))
            .force('x', d3.forceX(width / 2))
            .force('y', d3.forceY(height / 2))
            .on('tick', tick);

        // init D3 drag support
        const drag = d3.drag()
            // Mac Firefox doesn't distinguish between left/right click when Ctrl is held...
            .filter(() => d3.event.button === 0 || d3.event.button === 2)
            .on('start', (d) => {
                if (!d3.event.active) force.alphaTarget(0.3).restart();

                d.fx = d.x;
                d.fy = d.y;
            })
            .on('drag', (d) => {
                d.fx = d3.event.x;
                d.fy = d3.event.y;
            })
            .on('end', (d) => {
                if (!d3.event.active) force.alphaTarget(0);

                d.fx = null;
                d.fy = null;
            });

        // define arrow markers for graph links
        svg.append('svg:defs').append('svg:marker')
            .attr('id', 'end-arrow')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 6)
            .attr('markerWidth', 3)
            .attr('markerHeight', 3)
            .attr('orient', 'auto')
            .append('svg:path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', '#000');

        svg.append('svg:defs').append('svg:marker')
            .attr('id', 'start-arrow')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 4)
            .attr('markerWidth', 3)
            .attr('markerHeight', 3)
            .attr('orient', 'auto')
            .append('svg:path')
            .attr('d', 'M10,-5L0,0L10,5')
            .attr('fill', '#000');

        // line displayed when dragging new nodes
        const dragLine = svg.append('svg:path')
            .attr('class', 'link dragline hidden')
            .attr('d', 'M0,0L0,0');

        // handles to link and node element groups
        let path = svg.append('svg:g').selectAll('path');
        let circle = svg.append('svg:g').selectAll('g');

        // mouse event vars
        let selectedNode = null;
        let selectedLink = null;
        let mousedownLink = null;
        let mousedownNode = null;
        let mouseupNode = null;

        function resetMouseVars() {
            mousedownNode = null;
            mouseupNode = null;
            mousedownLink = null;
        }

        // update force layout (called automatically each iteration)
        function tick() {
            // draw directed edges with proper padding from node centers
            path.attr('d', (d) => {
                const deltaX = d.target.x - d.source.x;
                const deltaY = d.target.y - d.source.y;
                const dist = Math.sqrt(deltaX * deltaX + deltaY * deltaY);
                const normX = deltaX / dist;
                const normY = deltaY / dist;
                const sourcePadding = d.left ? 17 : 12;
                const targetPadding = d.right ? 17 : 12;
                const sourceX = d.source.x + (sourcePadding * normX);
                const sourceY = d.source.y + (sourcePadding * normY);
                const targetX = d.target.x - (targetPadding * normX);
                const targetY = d.target.y - (targetPadding * normY);

                return `M${sourceX},${sourceY}L${targetX},${targetY}`;
            });

            circle.attr('transform', (d) => `translate(${d.x},${d.y})`);
        }

        // update graph (called when needed)
        function restart() {
            // path (link) group
            path = path.data(self.links);

            // update existing links
            path.classed('selected', (d) => d === selectedLink)
                .style('marker-start', (d) => d.left ? 'url(#start-arrow)' : '')
                .style('marker-end', (d) => d.right ? 'url(#end-arrow)' : '');

            // remove old links
            path.exit().remove();

            // add new links
            path = path.enter().append('svg:path')
                .attr('class', 'link')
                .classed('selected', (d) => d === selectedLink)
                .style('marker-start', (d) => d.left ? 'url(#start-arrow)' : '')
                .style('marker-end', (d) => d.right ? 'url(#end-arrow)' : '')
                .on('mousedown', (d) => {
                    if (d3.event.ctrlKey) return;

                    // select link
                    mousedownLink = d;
                    selectedLink = (mousedownLink === selectedLink) ? null : mousedownLink;
                    selectedNode = null;
                    restart();
                })
                .merge(path);

            // circle (node) group
            // NB: the function arg is crucial here! nodes are known by id, not by index!
            circle = circle.data(self.nodes, (d) => d.id);

            // update existing nodes (reflexive & selected visual states)
            circle.selectAll('circle')
                .style('fill', (d) => (d === selectedNode) ? d3.rgb(colors(d.id)).brighter().toString() : colors(d.id))
                .classed('reflexive', (d) => d.reflexive);

            // remove old nodes
            circle.exit().remove();

            // add new nodes
            const g = circle.enter().append('svg:g');

            g.append('svg:circle')
                .attr('class', 'node')
                .attr('r', 12)
                .style('fill', (d) => (d === selectedNode) ? d3.rgb(colors(d.id)).brighter().toString() : colors(d.id))
                .style('stroke', (d) => d3.rgb(colors(d.id)).darker().toString())
                .classed('reflexive', (d) => d.reflexive)
                .on('mouseover', function (d) {
                    if (!mousedownNode || d === mousedownNode) return;
                    // enlarge target node
                    d3.select(this).attr('transform', 'scale(1.1)');
                })
                .on('mouseout', function (d) {
                    if (!mousedownNode || d === mousedownNode) return;
                    // unenlarge target node
                    d3.select(this).attr('transform', '');
                })
                .on('mousedown', (d) => {
                    if (d3.event.ctrlKey) return;

                    // select node
                    mousedownNode = d;
                    selectedNode = (mousedownNode === selectedNode) ? null : mousedownNode;
                    selectedLink = null;

                    // reposition drag line
                    dragLine
                        .style('marker-end', 'url(#end-arrow)')
                        .classed('hidden', false)
                        .attr('d', `M${mousedownNode.x},${mousedownNode.y}L${mousedownNode.x},${mousedownNode.y}`);

                    restart();
                })
                .on('mouseup', function (d) {
                    if (!mousedownNode) return;

                    // needed by FF
                    dragLine
                        .classed('hidden', true)
                        .style('marker-end', '');

                    // check for drag-to-self
                    mouseupNode = d;
                    if (mouseupNode === mousedownNode) {
                        resetMouseVars();
                        return;
                    }

                    // unenlarge target node
                    d3.select(this).attr('transform', '');

                    // add link to graph (update if exists) and select it
                    selectedLink = self.addLink(mousedownNode.id, mouseupNode.id);
                    selectedNode = null;
                    restart();
                });

            // show node IDs
            g.append('svg:text')
                .attr('x', 0)
                .attr('y', 4)
                .attr('class', 'id')
                .text((d) => d.id);

            circle = g.merge(circle);

            // set the graph in motion
            force
                .nodes(self.nodes)
                .force('link').links(self.links);

            force.alphaTarget(0.3).restart();
        }

        function mousedown() {
            // because :active only works in WebKit?
            svg.classed('active', true);

            if (d3.event.ctrlKey || mousedownNode || mousedownLink) return;

            // insert new node at point
            const point = d3.mouse(this);
            self.addNode(point[0], point[1]);

            restart();
        }

        function mousemove() {
            if (!mousedownNode) return;

            // update drag line
            dragLine.attr('d', `M${mousedownNode.x},${mousedownNode.y}L${d3.mouse(this)[0]},${d3.mouse(this)[1]}`);
        }

        function mouseup() {
            if (mousedownNode) {
                // hide drag line
                dragLine
                    .classed('hidden', true)
                    .style('marker-end', '');
            }

            // because :active only works in WebKit?
            svg.classed('active', false);

            // clear mouse event vars
            resetMouseVars();
        }


        // only respond once per keydown
        let lastKeyDown = -1;

        function keydown() {
            d3.event.preventDefault();

            if (lastKeyDown !== -1) return;
            lastKeyDown = d3.event.keyCode;

            // ctrl
            if (d3.event.keyCode === 17) {
                circle.call(drag);
                svg.classed('ctrl', true);
                return;
            }

            if (!selectedNode && !selectedLink) return;

            switch (d3.event.keyCode) {
                case 8: // backspace
                case 46: // delete
                    if (selectedNode) {
                        self.removeNode(selectedNode);
                    } else if (selectedLink) {
                        self.removeLink(selectedLink);
                    }
                    selectedLink = null;
                    selectedNode = null;
                    restart();
                    break;
                case 66: // B
                    if (selectedLink) {
                        // set link direction to both left and right
                        selectedLink.left = true;
                        selectedLink.right = true;
                    }
                    restart();
                    break;
                case 76: // L
                    if (selectedLink) {
                        // set link direction to left only
                        selectedLink.left = true;
                        selectedLink.right = false;
                    }
                    restart();
                    break;
                case 82: // R
                    if (selectedNode) {
                        // toggle node reflexivity
                        selectedNode.reflexive = !selectedNode.reflexive;
                    } else if (selectedLink) {
                        // set link direction to right only
                        selectedLink.left = false;
                        selectedLink.right = true;
                    }
                    restart();
                    break;
            }
        }

        function keyup() {
            lastKeyDown = -1;

            // ctrl
            if (d3.event.keyCode === 17) {
                circle.on('.drag', null);
                svg.classed('ctrl', false);
            }
        }

        // app starts here
        svg.on('mousedown', mousedown)
            .on('mousemove', mousemove)
            .on('mouseup', mouseup)
            .on('keydown', keydown)
            .on('keyup', keyup);

        restart();
    }
}

const svgs = [
    document.getElementById("left"),
    document.getElementById("right"),
]

const svg_height = svgs[0].getAttribute("height");
const svg_width = svgs[0].getAttribute("width");

const graphs = [
    new Graph(),
    new Graph(),
]

graphs[0].draw(svgs[0], svg_height, svg_width);
graphs[1].draw(svgs[1], svg_height, svg_width);

function argMax(a) {
    let I = 0;
    for (let i = 1; i < a.length; ++i) {
        if (a[i] > a[I]) {
            I = i;
        }
    }
    return I;
}

function createElement(tagName, innerHTML, background) {
    const element = document.createElement(tagName);
    if (innerHTML !== undefined) {
        element.innerHTML = innerHTML;
    }
    if (background !== undefined) {
        element.style.background = background;
    }
    return element;
}

function similarityMatrix() {
    const epsilon = 1e-5;
    // Reset table contents
    const table = document.getElementById("similarity_table");
    table.innerHTML = "";
    table.style.display = "none";

    const m = graphs[0].nodes.length;
    const n = graphs[1].nodes.length;
    if (m === 0 || n === 0) {
        return;
    }

    // Calculate similarity matrix
    const A = math.matrix(graphs[1].adjacencyMatrix(), 'sparse');
    const B = math.matrix(graphs[0].adjacencyMatrix(), 'sparse');
    const AT = math.transpose(A);
    const BT = math.transpose(B);
    let Z = math.ones(m, n);
    while (true) {
        const Z2 = Z;
        for (let i = 0; i < 2; ++i) {
            Z = math.add(math.multiply(B, Z, AT), math.multiply(BT, Z, A));
            Z = math.divide(Z, math.norm(Z, 'fro'));
        }
        if (math.norm(math.subtract(Z, Z2), 'fro') < epsilon) {
            break
        }
    }
    Z = Z.toArray();

    // Update similarity table
    const tr = document.createElement("tr");
    tr.appendChild(createElement("th"))
    for (const node of graphs[1].nodes) {
        tr.appendChild(createElement("th", node.id, colors(node.id)));
    }
    table.appendChild(tr);

    for (let i = 0; i < m; ++i) {
        const tr = document.createElement("tr");
        const z = argMax(Z[i])
        tr.appendChild(createElement("th", graphs[0].nodes[i].id, colors(graphs[0].nodes[i].id)));
        for (let j = 0; j < n; ++j) {
            tr.appendChild(createElement("td", Math.round((Z[i][j] + Number.EPSILON) * 1000) / 1000, j == z ? "#ddd" : undefined));
        }
        table.appendChild(tr);
    }
    table.style.display = "block";
}

let examples = [
    {
        name: "Self-similarity matrix 1",
        adjacency_matrix: [
            [
                [0, 1, 0],
                [0, 0, 1],
                [0, 0, 0],
            ],
            [
                [0, 1, 0],
                [0, 0, 1],
                [0, 0, 0],
            ]
        ],
    },
    {
        name: "Self-similarity matrix 2",
        adjacency_matrix: [
            [
                [0, 1, 0, 0],
                [0, 0, 1, 0],
                [0, 0, 0, 1],
                [1, 0, 0, 0],
            ],
            [
                [0, 1, 0, 0],
                [0, 0, 1, 0],
                [0, 0, 0, 1],
                [1, 0, 0, 0],
            ]
        ]
    },
    {
        name: "Self-similarity matrix 3",
        adjacency_matrix: [
            [
                [0, 1, 0, 0],
                [0, 0, 1, 1],
                [0, 0, 0, 0],
                [0, 0, 0, 0],
            ],
            [
                [0, 1, 0, 0],
                [0, 0, 1, 1],
                [0, 0, 0, 0],
                [0, 0, 0, 0],
            ]
        ],
    },
    {
        name: "Central score 1",
        adjacency_matrix: [
            [
                [0, 1, 1, 0, 0],
                [0, 0, 1, 0, 1],
                [0, 0, 0, 1, 1],
                [0, 1, 0, 0, 0],
                [0, 0, 0, 0, 0],
            ],
            [
                [0, 1, 0],
                [0, 0, 1],
                [0, 0, 0],
            ],
        ]
    },
    {
        name: "Bow-tie graf",
        adjacency_matrix: [
            [
                [0,0,0,0,0,0,1,1,1,1,1],
                [1,0,0,0,0,0,0,0,0,0,0],
                [1,0,0,0,0,0,0,0,0,0,0],
                [1,0,0,0,0,0,0,0,0,0,0],
                [1,0,0,0,0,0,0,0,0,0,0],
                [1,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0]
            ],
            [
                [0,1],
                [0,0]
            ]
        ]
    },
    {
        name: "Česti vrhovi",
        adjacency_matrix: [
            [
                [0,1,1,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0,0,0],
                [0,1,1,0,0,0,0,0,0,0,0,0,0],
                [0,1,1,0,0,0,0,0,0,0,0,0,0],
                [0,1,1,0,0,0,0,0,0,0,0,0,0],
                [1,0,0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,1,0,0,0,0,0,0,0,0,0],
                [0,0,0,1,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,1,0,0,0,0,0,0,0,0],
                [0,0,0,0,1,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,1,0,0,0,0,0,0,0],
                [0,0,0,0,0,1,0,0,0,0,0,0,0]
            ],
            [
                [0,1,0],
                [0,0,1],
                [0,0,0]
            ],
        ],
    }

]

exampleSelect = document.getElementById("examples");
for (let i = 0; i < examples.length; ++i) {
    const option = document.createElement("option");
    option.value = i;
    option.innerHTML = examples[i].name;
    exampleSelect.appendChild(option);
}

function resetGraph(k, m) {
    graphs[k] = new Graph();
    if (m !== undefined) {
        for (let i = 0; i < m.length; ++i) {
            graphs[k].addNode(0, 0);
        }
        for (let i = 0; i < m.length; ++i) {
            for (let j = 0; j < m[i].length; ++j) {
                if (m[i][j] !== 0) {
                    graphs[k].addLink(i, j);
                }
            }
        }
    }
    const new_svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    graphs[k].draw(new_svg, svg_height, svg_width);
    svgs[k].replaceWith(new_svg);
    svgs[k] = new_svg;
}


function loadExample(t) {
    if (t.value >= 0) {
        for (let k = 0; k < 2; ++k) {
            resetGraph(k, examples[t.value].adjacency_matrix[k]);
        }
        similarityMatrix();
    }
}

const help = document.getElementById("help");

function openHelp() {
    help.style.display = "flex";
}

function closeHelp() {
    help.style.display = "none";
}

function adjacencyMatrixStr(t) {
    const A = graphs[t].adjacencyMatrix()
    if (A.length === 0) {
        return "[]";
    }
    let s = "[" + JSON.stringify(A[0])
    for (let i = 1; i < A.length; ++i) {
        s += ",\n"
        s += JSON.stringify(A[i]);
    }
    return s + "]"
}
