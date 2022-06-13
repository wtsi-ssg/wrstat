define(["d3", "cookie"], function (d3, cookie) {
    console.log("dtreemap using d3 v" + d3.version);

    var current;
    let allNodes = new Map();

    var margin = { top: 20, right: 0, bottom: 0, left: 0 },
        width = 960,
        height = 500 - margin.top - margin.bottom,
        transitioning;

    var x = d3.scale.linear()
        .domain([0, width])
        .range([0, width]);

    var y = d3.scale.linear()
        .domain([0, height])
        .range([0, height]);

    var treemap = d3.layout.treemap()
        .sort(function (a, b) { return a.value - b.value; })
        .ratio(height / width * 0.5 * (1 + Math.sqrt(5)))
        .round(false);

    var svg = d3.select("#chart").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.bottom + margin.top)
        .style("margin-left", -margin.left + "px")
        .style("margin.right", -margin.right + "px")
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
        .style("shape-rendering", "crispEdges");

    var grandparent = svg.append("g")
        .attr("class", "grandparent");

    grandparent.append("rect")
        .attr("y", -margin.top)
        .attr("width", width)
        .attr("height", margin.top);

    grandparent.append("text")
        .attr("x", 6)
        .attr("y", 6 - margin.top)
        .attr("dy", ".75em");

    function initialize(root) {
        root.x = root.y = 0;
        root.dx = width;
        root.dy = height;
        root.depth = 0;
    }

    var filterIDs = ['#select_group', '#select_user', '#select_filetype'];
    let filters = new Map();
    filterIDs.forEach(id => filters.set(id, "*"));

    function getFilters() {
        str = "";
        filterIDs.forEach(id => str += id + ':' + filters.get(id) + ';');
        return str;
    }

    function storeFilters(node) {
        currentFilters = getFilters();
        node.filters = currentFilters;

        if (node.children) {
            node.children.forEach(child => child.filters = currentFilters);
        }
    }

    // Compute the treemap layout recursively such that each group of siblings
    // uses the same size (1×1) rather than the dimensions of the parent cell.
    // This optimizes the layout for the current zoom state. Note that a wrapper
    // object is created for the parent node for each group of siblings so that
    // the parent’s dimensions are not discarded as we recurse. Since each group
    // of sibling was laid out in 1×1, we must rescale to fit using absolute
    // coordinates. This lets us use a viewport to zoom.
    function layout(d) {
        var children = getChildren(d);
        if (children && children.length > 0) {
            treemap.nodes({ children: children });
            children.forEach(function (c) {
                c.x = d.x + c.x * d.dx;
                c.y = d.y + c.y * d.dy;
                c.dx *= d.dx;
                c.dy *= d.dy;
                c.parent = d;
                layout(c);
            });
        }
    }

    function display(d) {
        grandparent
            .datum(d.parent)
            .on("click", transition)
            .on("mouseover", showCurrentDetails)
            .style("cursor", "pointer")
            .select("text")
            .text(path(d));

        var g1 = svg.insert("g", ".grandparent")
            .datum(d)
            .attr("class", "depth");

        var g = g1.selectAll("g")
            .data(getChildren(d))
            .enter().append("g");

        g.filter(function (d) { return d.has_children; })
            .classed("children", true)
            .style("cursor", "pointer")
            .on("click", transition);

        g.selectAll(".child")
            .data(function (d) { return getChildren(d) || [d]; })
            .enter().append("rect")
            .attr("class", "child")
            .call(rect);

        g.append("rect")
            .attr("class", "parent")
            .call(rect)
            .append("title");

        g.append("text")
            .attr("dy", ".75em")
            .text(function (d) { return d.name; })
            .call(text);

        g.on("mouseover", mouseover).on("mouseout", mouseout);

        // var parent_and_children = g1.selectAll("g.parent_and_children")
        //     .data(getChildren(d))
        //     .enter().append("svg:g");

        // parent_and_children
        //     .classed("parent_and_children", true)
        //     .on("mouseover", mouseover)
        //     .on("mouseout", mouseout);

        // parent_and_children
        //     .on("click", transition);

        // parent_and_children.selectAll(".child")
        //     .data(function (d) {
        //         return d.children || [d];
        //     })
        //     .enter().append("rect")
        //     .classed("child", true)
        //     .call(treebox)
        //     .style("fill", "red");

        // parent_and_children.append("rect")
        //     .classed("parent", true)
        //     .call(treebox)
        //     .style("fill", "blue");

        // var titlesvg = parent_and_children.append("svg")
        //     .classed("parent_title", true)
        //     .attr("viewBox", "-100 -10 200 20")
        //     .attr("preserveAspectRatio", "xMidYMid meet")
        //     .call(treebox);

        // titlesvg.append("text")
        //     .attr("font-size", 16)
        //     .attr("x", 0)
        //     .attr("y", 0)
        //     .attr("width", 200)
        //     .attr("height", 20)
        //     .attr("dy", ".3em")
        //     .style("text-anchor", "middle")
        //     .text(function (d) {
        //         return d.name;
        //     });

        d3.selectAll("#select_area input").on("change", function () {
            console.log("area changed to ", this.value);
            areaBasedOnSize = this.value == "size"
            setAllValues()
            transition(current);
        });

        filterIDs.forEach(id => d3.select(id).on('change', function () {
            selectValue = d3.select(this).property('value');
            filters.set(id, selectValue);
            getData(current.path, function (data) {
                updated = data.root;
                current.size = updated.size;
                current.count = updated.count;
                current.children = updated.children;
                current.has_children = updated.has_children;
                setValues(current);
                storeFilters(current);
                transition(current);
                updateDetails(current);
            });
        }));

        function mouseover(g) {
            showDetails(g)
        }

        function mouseout() {
            showCurrentDetails()
        }

        function transition(d) {
            if (transitioning || !d) return;
            transitioning = true;

            function do_transition(d) {
                layout(d);

                var g2 = display(d),
                    t1 = g1.transition().duration(250),
                    t2 = g2.transition().duration(250);

                // Update the domain only after entering new elements.
                x.domain([d.x, d.x + d.dx]);
                y.domain([d.y, d.y + d.dy]);

                // Enable anti-aliasing during the transition.
                svg.style("shape-rendering", null);

                // Draw child nodes on top of parent nodes.
                svg.selectAll(".depth").sort(function (a, b) { return a.depth - b.depth; });

                // Fade-in entering text.
                g2.selectAll("text").style("fill-opacity", 0);

                // Transition to the new view.
                // t1.selectAll(".parent_title").call(treebox);
                // t2.selectAll(".parent_title").call(treebox);
                t1.selectAll("text").call(text).style("fill-opacity", 0);
                t2.selectAll("text").call(text).style("fill-opacity", 1);
                t1.selectAll("rect").call(rect);
                t2.selectAll("rect").call(rect);

                // Remove the old node when the transition is finished.
                t1.remove().each("end", function () {
                    svg.style("shape-rendering", "crispEdges");
                    transitioning = false;
                });
            }

            if (d.children && d.filters == getFilters()) {
                do_transition(d)
            } else {
                // console.log('getting fresh data for ', d.path);
                getData(d.path, function (data) {
                    d.children = data.root.children;
                    setValues(d);
                    storeFilters(d);
                    do_transition(d)
                });
            }

            updateDetails(d)
        }

        return g;
    }

    function text(text) {
        text.attr("x", function (d) { return x(d.x) + 6; })
            .attr("y", function (d) { return y(d.y) + 6; });
    }

    function rect(rect) {
        rect.attr("x", function (d) { return x(d.x); })
            .attr("y", function (d) { return y(d.y); })
            .attr("width", function (d) { return x(d.x + d.dx) - x(d.x); })
            .attr("height", function (d) { return y(d.y + d.dy) - y(d.y); });
    }

    // function treebox(b) {
    //     b.attr("x", function (d) {
    //         //console.log("treebox: d.x=", d.x, " x(d.x)=", x(d.x), " d=", d);
    //         return x(d.x);
    //     })
    //         .attr("y", function (d) {
    //             return y(d.y);
    //         })
    //         .attr("width", function (d) {
    //             return x(d.x + d.dx) - x(d.x);
    //         })
    //         .attr("height", function (d) {
    //             return y(d.y + d.dy) - y(d.y);
    //         });
    // }

    function path(d) {
        return d.path;
    }

    function getChildren(parent) {
        if (!parent.has_children) {
            return
        }

        if (parent.children) {
            return parent.children
        }
    }

    function constructAPIURL(path) {
        url = "/rest/v1/auth/tree?path=" + path

        group = d3.select('#select_group').property('value');
        user = d3.select('#select_user').property('value');
        filetype = d3.select('#select_filetype').property('value');

        if (group != "*") {
            url += '&groups=' + group
        }

        if (user != "*") {
            url += '&users=' + user
        }

        if (filetype != "*") {
            url += '&types=' + filetype
        }

        return url
    }

    function getData(path, loadFunction) {
        d3.select("#spinner").style("display", "inline-block")

        d3.json(constructAPIURL(path))
            .header("Authorization", "Bearer " + cookie.get('jwt'))
            .on("error", function (error) {
                d3.select("#spinner").style("display", "none")
                d3.select("#error").text("error: " + error);
            })
            .on("load", function (data) {
                d3.select("#spinner").style("display", "none");
                allNodes.set(data.root.path, data.root);
                setFilterOptions(data);
                loadFunction(data);
            })
            .get();
    }

    var areaBasedOnSize = true

    function setValues(d) {
        if (areaBasedOnSize) {
            d.value = d.size;

            if (d.children) {
                d.children.forEach(item => item.value = item.size);
            }
        } else {
            d.value = d.count;

            if (d.children) {
                d.children.forEach(item => item.value = item.count);
            }
        }
    }

    function setAllValues() {
        for (let d of allNodes.values()) {
            setValues(d);
        }
    }

    var BINARY_UNIT_LABELS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];

    function bytesHuman(bytes) {
        var e = Math.floor(Math.log(bytes) / Math.log(1024));
        return parseFloat((bytes / Math.pow(1024, e)).toFixed(2)) + " " + BINARY_UNIT_LABELS[e];
    }

    var NUMBER_UNIT_LABELS = ["", "K", "M", "B", "T", "Q"];

    function countHuman(count) {
        var unit = Math.floor((count / 1.0e+1).toFixed(0).toString().length)
        var r = unit % 3
        var x = Math.abs(Number(count)) / Number('1.0e+' + (unit - r)).toFixed(2)
        return parseFloat(x.toFixed(2)) + ' ' + NUMBER_UNIT_LABELS[Math.floor(unit / 3)]
    }

    function showDetails(node) {
        d3.select('#details_path').text(node.path)
        d3.select('#details_size').text(bytesHuman(node.size))
        d3.select('#details_count').text(countHuman(node.count))
    }

    function showCurrentDetails() {
        showDetails(current)
    }

    function updateDetails(node) {
        current = node
        showCurrentDetails()
    }

    function setFilterOptions(data) {
        setFilter('#select_group', data.groups);
        setFilter('#select_user', data.users);
        setFilter('#select_filetype', data.filetypes);
    }

    function setFilter(id, elements) {
        elements.unshift("*")

        var select = d3.select(id)

        select
            .selectAll('option')
            .remove();

        select
            .selectAll('option')
            .data(elements).enter()
            .append('option')
            .text(function (d) { return d; })
            .property("selected", function (d) { return d === filters.get(id) });
    }

    getData("/", function (data) {
        root = data.root;
        initialize(root);
        setValues(root);
        storeFilters(root);
        layout(root);
        display(root);
        updateDetails(root);
    });
});