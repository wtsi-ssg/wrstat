require.config({
    paths: {
        cookie: "js.cookie.min"
    }
})

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

    $('.flexdatalist').flexdatalist({
        selectionRequired: 1,
        minLength: 0
    });

    var $filter_inputs = $('.flexdatalist');

    var filterIDs = ['groups_list', 'users_list', 'ft_list'];
    let filters = new Map();

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
            .select("text")
            .text(d.path);

        if (d.path == "/") {
            grandparent.style("cursor", "default")
        } else {
            grandparent.style("cursor", "pointer")
        }

        var g1 = svg.insert("g", ".grandparent")
            .datum(d)
            .classed("depth", true);

        var g = g1.selectAll("g")
            .data(getChildren(d))
            .enter().append("g");

        g.filter(function (d) { return d.has_children; })
            .classed("children", true)
            .style("cursor", "pointer")
            .on("click", transition);

        g.filter(function (d) { return !d.has_children; })
            .classed("childless", true)
            .style("cursor", "default");

        g.selectAll(".child")
            .data(function (d) { return getChildren(d) || [d]; })
            .enter().append("rect")
            .classed("child", true)
            .call(rect);

        g.append("rect")
            .classed("parent", true)
            .call(rect);

        var titlesvg = g.append("svg")
            .classed("parent_title", true)
            .attr("viewBox", "-100 -10 200 20")
            .attr("preserveAspectRatio", "xMidYMid meet")
            .call(rect);

        titlesvg.append("text")
            .attr("font-size", 16)
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", 200)
            .attr("height", 20)
            .attr("dy", ".3em")
            .style("text-anchor", "middle")
            .text(function (d) { return d.name; });

        g.on("mouseover", mouseover).on("mouseout", mouseout);

        d3.selectAll("#select_area input").on("change", function () {
            areaBasedOnSize = this.value == "size"
            setAllValues()
            transition(current);
        });

        d3.select("#filterButton").on('click', function () {
            getData(current.path, function (data) {
                current.size = data.size;
                current.count = data.count;
                current.groups = data.groups;
                current.users = data.users;
                current.filetypes = data.filetypes;
                current.children = data.children;
                current.has_children = data.has_children;
                setValues(current);
                storeFilters(current);
                transition(current);
            });
        });

        var updateFilters = function ($target) {
            $target.each(function () {
                filters.set($(this).attr('id'), $(this).val());
            });
        };

        $filter_inputs
            .on('change:flexdatalist', function (e, set) {
                updateFilters($(this));
            });

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
                t1.selectAll(".parent_title").call(rect);
                t2.selectAll(".parent_title").call(rect);
                t1.selectAll("text").style("fill-opacity", 0);
                t2.selectAll("text").style("fill-opacity", 1);
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
                    d.children = data.children;
                    setValues(d);
                    storeFilters(d);
                    do_transition(d)
                });
            }

            updateDetails(d);
            setFilterOptions(d);
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

    function getChildren(parent) {
        return parent.children
    }

    function constructAPIURL(path) {
        url = "/rest/v1/auth/tree?path=" + path

        groups = d3.select('#groups_list').property('value');
        users = d3.select('#users_list').property('value');
        filetypes = d3.select('#ft_list').property('value');

        if (groups != "") {
            url += '&groups=' + groups
        }

        if (users != "") {
            url += '&users=' + users
        }

        if (filetypes != "") {
            url += '&types=' + filetypes
        }

        return url
    }

    function getData(path, loadFunction) {
        d3.select("#spinner").style("display", "inline-block")

        fetch(constructAPIURL(path), {
            headers: {
                "Authorization": `Bearer ${cookie.get('jwt')}`
            }
        }).then(r => {
            if (r.status === 401) {
                cookie.remove("jwt", { path: "" });
                window.location.reload();
            } else {
                return r.json()
            }
        }).then(d => {
            d3.select("#spinner").style("display", "none");
            if (d.count > 0) {
                d3.select("#error").text("")
                allNodes.set(d.path, d);
                loadFunction(d);
            }
            else {
                d3.select("#error").text("error: no results")
            }

        }).catch(e => {
            d3.select("#spinner").style("display", "none");
            d3.select("#error").text("error: " + e)
        })

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
        var unit = Math.floor((count / 1.0e+1).toFixed(0).toString().length);
        var r = unit % 3;
        var x = Math.abs(Number(count)) / Number('1.0e+' + (unit - r)).toFixed(2);
        return parseFloat(x.toFixed(2)) + ' ' + NUMBER_UNIT_LABELS[Math.floor(unit / 3)];
    }

    function commaSep(list) {
        return list.join(", ");
    }

    function showDetails(node) {
        d3.select('#details_path').text(node.path);
        d3.select('#details_size').text(bytesHuman(node.size));
        d3.select('#details_count').text(countHuman(node.count));
        d3.select('#details_atime').text(node.atime);
        d3.select('#details_groups').text(commaSep(node.groups));
        d3.select('#details_users').text(commaSep(node.users));
        d3.select('#details_filetypes').text(commaSep(node.filetypes));
    }

    function showCurrentDetails() {
        showDetails(current);
    }

    function updateDetails(node) {
        current = node;
        showCurrentDetails();
    }

    function setFilterOptions(data) {
        setFilter('#groups_list', data.groups);
        setFilter('#users_list', data.users);
        setFilter('#ft_list', data.filetypes);
    }

    function setFilter(id, elements) {
        var select = d3.select(id);

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

    function setTimestamp(data) {
        $('#timestamp').attr('datetime', data.timestamp)
        $('#timestamp').timeago();
    }

    getData("/", function (data) {
        initialize(data);
        setValues(data);
        layout(data);
        display(data);
        updateDetails(data);
        setFilterOptions(data);
        storeFilters(data);
        setTimestamp(data);
    });
});