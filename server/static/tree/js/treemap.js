define(["d3", "lodash", "queue"], function(d3, _, queue) {
    console.log("treemap using d3 v" + d3.version + ", underscore v" + _.VERSION + ", queue v" + queue.version);

    var BINARY_UNIT_LABELS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];
    var SI_UNIT_LABELS = ["", "k", "M", "G", "T", "P", "E", "Z", "Y"];
    var HUMAN_TRIGGER_RATIO = 0.8;
    var OUTPUT_UNIT_SEP = " ";
    var HTTP_RETRY_COUNT = 5;

    var _root = new Object();
    var dates = new Array();
    var treemap = {
        get root() {
            return _root;
        },
        set root(d) {
            _root = d;
        },
        loaded_paths: new Array(),
        nodes: new Array(),
        layout: undefined,
        svg: undefined,
        grandparent: undefined,
        fillScale: "linear",
        sizeAccessor: undefined,
        fillAccessor: undefined,
        size: {
            metric: "size",
            group: "*",
            user: "*",
            tag: "*"
        },
        fill: {
            metric: "count",
            group: "*",
            user: "*",
            tag: "*"
        },
        loading: false,
        color: undefined,
        genColorScale: function(d, fillAccessor) {
            // colors from colorbrewer2.org 5-class YlGnBu
            low = "#ffffcc";
            lmid = "#a1dab4";
            mid = "#41b6c4";
            hmid = "#2c7fb8";
            high = "#253494";

            //console.log("genColorScale: d=", d);
            var nodes = d.child_dirs;

            var min = d3.min(nodes, fillAccessor);
            var max = d3.max(nodes, fillAccessor);
            //console.log("genColorScale: min=", min, " max=", max, " for nodes=", nodes, " using fillAccessor=", fillAccessor);

            if(treemap.fillScale == "linear") {
                if(min > 0) {
                    min = -1;
                }
                if(max < 0) {
                    max = 1;
                }
                return d3.scale.linear()
                    .interpolate(d3.interpolateHsl)
                    .domain([min, min / 2, 0, max / 2, max])
                    .range([low, lmid, mid, hmid, high])
                    .nice();
            } else {
                if(min <= 0) {
                    min = 0.001;
                }
                if(max <= 0) {
                    max = 1;
                }
                var log_min = Math.floor(Math.log(min));
                var log_max = Math.ceil(Math.log(max));
                var log_range = log_max - log_min;
                var log_mid = Math.floor(log_min + (log_range / 2));
                var log_lmid = Math.floor(log_min + ((log_mid - log_min) / 2));
                var log_hmid = Math.floor(log_max - ((log_mid - log_min) / 2));
                //console.log("genColorScale: log domain=", Math.pow(10, log_min), Math.pow(10, log_lmid), Math.pow(10, log_mid), Math.pow(10, log_hmid), Math.pow(10, log_max))
                return d3.scale.log()
                    .interpolate(d3.interpolateHsl)
                    .domain([Math.pow(10, log_min), Math.pow(10, log_lmid), Math.pow(10, log_mid), Math.pow(10, log_hmid), Math.pow(10, log_max)])
                    .range([low, lmid, mid, hmid, high])
                    .nice();
            }
        }
    };

    var margin = {top: 20, right: 0, bottom: 20, left: 0},
        width = 960 - margin.right - margin.left,
        height = 500 - margin.top - margin.bottom,
        formatNumber = d3.format(",d"),
        transitioning;

    var x = d3.scale.linear()
        .domain([0, width])
        .range([0, width])
        .nice();

    var y = d3.scale.linear()
        .domain([0, height])
        .range([0, height])
        .nice();

//    var title = d3.select("#footer").insert("div", ":first-child");

    var node;

    var curg; // current g element

    function getValueAccessor(metric, filters) {
        return function(d) {
            //console.log("calling valueAccessor on d=", d, " with metric=", params.metric, " group=", params.group, " user=", params.user, " tag=", params.tag);
            return valueAccessor(d, metric, filters);
        }
    }

    treemap.sizeAccessor = getValueAccessor(treemap.size.metric, treemap.size);
    console.log("treemap.sizeAccessor set to: ", treemap.sizeAccessor);

    treemap.fillAccessor = getValueAccessor(treemap.fill.metric, treemap.fill);
    console.log("treemap.fillAccessor set to: ", treemap.fillAccessor);

    var fillColor = function(d) {
        var value = treemap.fillAccessor(d);
        var hsl = treemap.color(value);
        //console.log("fillColor: value=", value, " hsl=", hsl, " for d=", d);
        return hsl;
    };

    var sizeValue = function(d) {
        var value = treemap.sizeAccessor(d);
        //console.log("sizeValue: value=", value, " for d=", d);
        if(_.isNaN(value)) {
            value = -1;
        }
        return value;
    };

    function addDate(date) {
        var tmp_dates = dates;
        dates = _.union(dates, [date]);
        if(!_.isEqual(tmp_dates, dates)) {
            console.log("dates updated to ", dates);
            d3.select("#dates").text(dates.join());
        }
    }

    function displayMetric(metric) {
        var display = metric;
        if(display == "ctime") {
            display = "Cost since creation";
        } else if(display == "atime") {
            display = "Cost since last accessed";
        } else if(display == "mtime") {
            display = "Cost since last modified";
        } else {
            display = _.capitalize(display);
        }
        return display;
    }

    function displayFilters(filters) {
        var display_filters = "";
        if(_.isUndefined(filters)) {
            filters = new Object();
            filters.group = "*";
            filters.user = "*";
            filters.tag = "*";
        }
        var limits = [];
        if(!_.isUndefined(filters.group) && filters.group != "*") {
            limits.push("g:" + filters.group);
        }
        if(!_.isUndefined(filters.user) && filters.user != "*") {
            limits.push("u:" + filters.user);
        }
        if(!_.isUndefined(filters.tag) && filters.tag != "*") {
            limits.push("t:" + filters.tag);
        }

        if(limits.length > 0) {
            display_filters = " (" + limits.join(" & ") + ")";
        }
        return display_filters;
    }

    function displayKey(metric, filters) {
        var display_key = displayMetric(metric);
        display_key += displayFilters(filters);
        return display_key;
    }

    function valueAccessor(d, metric, filters) {
        //console.log("valueAccessor: d=", d, " metric=", metric, " group=", group, " user=", user, " tag=", tag)
        if(_.isUndefined(metric)) {
            console.log("valueAccessor: ERROR metric undefined for d.data=", d.data);
            return -1;
        }
        if(metric == "path") {
            return d.path;
        }
        if(metric == "name") {
            return d.name;
        }
        if(_.isUndefined(filters)) {
            filters = new Object();
            filters.group = "*";
            filters.user = "*";
            filters.tag = "*";
        }
        if(_.isObject(d.data)) {
            if(!(metric in d.data)) {
                console.log("valueAccessor: ERROR metric=", metric, " not in d=", d);
                return -1;
            }
            if(!(filters.group in d.data[metric])) {
                //console.log("valueAccessor: ERROR group=", filters.group, " not in d.data[", metric, "]=", d.data[metric]);
                return 0;
            }
            if(!(filters.user in d.data[metric][filters.group])) {
                //console.log("valueAccessor: ERROR user=", filters.user, " not in d.data[", metric, "][", filters.group, "]=", d.data[metric][filters.group]);
                return 0;
            }
            if(!(filters.tag in d.data[metric][filters.group][filters.user])) {
                //console.log("valueAccessor: ERROR tag=", filters.tag, " not in d.data[", metric, "][", filters.group, "][", filters.user, "]=", d.data[metric][filters.group][filters.user]);
                return 0;
            }
            var value = d.data[metric][filters.group][filters.user][filters.tag];
            if(_.isUndefined(value)) {
                console.log("valueAccessor: ERROR undefined value for metric=", metric, " group=", filters.group, " user=", filters.user, " tag=", filters.tag);
                return -1;
            }
            return +value;
        } else {
            // d.data not an object
            console.log("valueAccessor: ERROR d.data not an object. d=", d);
            return -1;
        }
    }

    function displayValue(d, metric, filters) {
        if(_.isUndefined(metric)) {
            console.log("displayValue: ERROR metric undefined for d=", d);
            return "ERROR";
        }
        if(_.isUndefined(filters)) {
            filters = new Object();
            filters.group = "*";
            filters.user = "*";
            filters.tag = "*";
        }
        var value = valueAccessor(d, metric, filters);
        if(/^size$/.exec(metric)) {
            return bytes_to_human_readable_string(value);
        } else if(/time$/.exec(metric)) { // really cost - TODO: fix server to say "atime_cost" etc
            return "£" + value.toFixed(2);
        } else if(/^count$/.exec(metric)) {
            return count_to_human_readable_string(value);
        } else {
            return value;
        }
    }

    treemap.layout = d3.layout.treemap()
        .children(function(d, depth) {
            return depth ? null : d.child_dirs;
        })
        //    .size([960, 500])
        .sort(function(a, b) {
            return a.value - b.value;
        })
        //    .ratio(height / width * 0.5 * (1 + Math.sqrt(5)))
        //    .ratio(width/height * 0.5 * (1 + Math.sqrt(5)))
        .round(false);

    treemap.svg = d3.select("#chart").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.bottom + margin.top)
        .style("margin-left", -margin.left + "px")
        .style("margin.right", -margin.right + "px")
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
        .style("shape-rendering", "crispEdges");

    treemap.grandparent = treemap.svg.append("g")
        .classed("grandparent", true);

    treemap.grandparent.append("rect")
        .attr("y", -margin.top)
        .attr("width", width)
        .attr("height", margin.top);

    treemap.grandparent.append("text")
        .attr("x", 6)
        .attr("y", 6 - margin.top)
        .attr("dy", ".75em");

    var path_data_url_templates = {
        //"/lustre": _.template("../api/lustretree/test?depth=2&path=<%= path %>"),
        //"/lustre/scratch112": _.template("../api/lustretree/scratch112/v2?depth=2&path=<%= path %>"),
        //"/lustre/scratch113": _.template("../api/lustretree/scratch113/v2?depth=2&path=<%= path %>"),
        "/lustre/": _.template("./api/v2?depth=1&path=<%= path %>"),
    };

    function reloadButIgnoreScratchesThatCannotBeLoaded(retry_d, treemap, onload_cb) {
        failed = [];
        complete = 0;

        function createCallback(key, onComplete) {
            return function(error) {
                if(error) {
                    failed.push(key)
                }
                complete++;
                if(complete == _.size(path_data_url_templates)) {
                    onComplete(failed);
                }
            }
        }
        function onComplete(failed) {
            _.forEach(failed, function(value) {
                delete path_data_url_templates[value]
            });
            displayError("No data loaded for: " + failed.join(", "));
            fetchTreemapData(retry_d, treemap, onload_cb);
        }

        _.forEach(path_data_url_templates, function(value, key) {
            url = path_data_url_templates[key](retry_d);
            d3.json(url).get(createCallback(key, onComplete));
        });
    }

    function startLoading() {
        if(!treemap.loading) {
            treemap.loading = true;
            d3.select("#status").text("Loading...");
            d3.select("body").style("cursor", "wait");
            return treemap.loading;
        } else {
            // already loading
            return false;
        }
    }

    function isLoading() {
        return treemap.loading;
    }

    function stopLoading() {
        treemap.loading = false;
        d3.select("#status").text("");
        d3.select("body").style("cursor", "default");
    }

    function displayError(error) {
        d3.select("#error").text(error);
    }

    function queueTreemapDataRequests(d) {
        //console.log("queueTreemapDataRequests d=", d);
        var q = queue();
        _.forEach(path_data_url_templates, function(n, root_path) {
            if(_.startsWith(d.path, root_path) || _.startsWith(root_path, d.path)) {
                url = path_data_url_templates[root_path](d);
                console.log("queueTreemapDataRequests: requesting url=" + url);
                q.defer(d3.json, url);
            }
        });
        return q;
    }

    function awaitTreemapDataRequests(q, retry_d, treemap, onload_cb) {
        //console.log("awaitTreemapDataRequests d=", d, " old_root=", old_root);
        q.await(function() {
            //console.log("awaitTreemapDataRequests: have treemap.root:", treemap.root);
            error = _(arguments).shift();
            data = arguments;
            if(error) {
                function allowAttemptToIgnoreFailures() {
                    function wrap() {
                        displayError("Detecting working scratch data sources...");
                        stopLoading();
                        reloadButIgnoreScratchesThatCannotBeLoaded(retry_d, treemap, onload_cb)
                    }
                    // Touching the DOM directly - old school! No jQuery, although perhaps d3 could do this
                    document.getElementById("error").appendChild(document.createElement("br"));
                    button = document.createElement("button");
                    button.innerHTML = "Attempt to ignore load failures";
                    button.addEventListener("click", wrap);
                    document.getElementById("error").appendChild(button);
                }

                if(error.status == 401) {
                    console.log("Client unauthorized (" + error.status + " ", error.statusText, "): ", error.responseText);
                    console.log("Should handle authorization TODO!");
                    stopLoading();
                } else if(error.status >= 500 && error.status < 600) {
                    console.log("Server error " + error.status + " (", error.statusText, "): ", error.responseText);
                    http_retries--;
                    if(http_retries > 0) {
                        console.log("Retrying request for retry_d=", retry_d);
                        displayError("Retrying... (" + http_retries + ")");
                        stopLoading();
                        fetchTreemapData(retry_d, treemap, onload_cb);
                    } else {
                        console.log("No more retries! Giving up.");
                        console.log("Should report this to user TODO!");
                        stopLoading();
                        displayError("Error: no more retries, giving up (please reload)");
                        allowAttemptToIgnoreFailures();
                    }
                } else if(error.status >= 400 && error.status < 500) {
                    console.log("Client error (" + error.status + " ", error.statusText, "): ", error.responseText);
                    console.log("Should print to page TODO!");
                    stopLoading();
                    displayError("Error: " + error.statusText);
                    allowAttemptToIgnoreFailures();
                } else if(error.status == 0) {
                    console.log("CORS error, possibly from shibboleth redirect?");
                    console.log(error.getAllResponseHeaders());
                    stopLoading();
                    displayError("CORS error (please reload)");
                    //TODO fix reload to take us back where we were
                } else {
                    console.log("Unexpected error ", error.readyState, error.response, error.responseText, error.responseType, error.responseXML, error.status, error.statusText, error.timeout, error.withCredentials);
                    console.log(error.getAllResponseHeaders());
                    stopLoading();
                    displayError("Unexpected error (please reload)");
                    //TODO fix reload to take us back where we were
                    window.location.reload();
                }
            } else { // successful result
                http_retries = HTTP_RETRY_COUNT;
                if(_.every(data, _.isObject) && data.length >= 1) {
                    if(_.isEmpty(treemap.root)) {
                        var root = {
                            name: "",
                            path: "/",
                            child_dirs: new Array(),
                            data: {
                                atime: {'*': {'*': {'*': 0}}},
                                ctime: {'*': {'*': {'*': 0}}},
                                mtime: {'*': {'*': {'*': 0}}},
                                count: {'*': {'*': {'*': 0}}},
                                size: {'*': {'*': {'*': 0}}}
                            }
                        };
                        console.log("treemap.root is empty: ", treemap.root, "creating root node: ", root);
                        treemap.root = root;
                    }
                    _.forEach(data, function(d) {
                        console.log("merging d dated ", d.date, " into treemap.root. d.tree=", d.tree);
                        addDate(d.date);
                        mergeLustreTree(treemap.root, d.tree);
                        treemap.loaded_paths.push(d.tree.path);
                    });

                    //initialDataLoad();
                    var result = _.attempt(onload_cb);

                    if(_.isError(result)) {
                        console.log("Error invoking onload_cb ", onload_cb);
                        console.log("error was ", result.name, ": ", result.message, "(", result.fileName, " line ", result.lineNumber, ")");
                    }
                    // clear error message
//            displayError("");
                } else {
                    console.log("queue completed but data.tree missing. have data.tree=", data.tree);
                }
            }
            stopLoading();
        });
    }

    function fetchTreemapData(d, treemap, load_callback) {
        //console.log("fetchTreemapData d=", d, "treemap.root=", treemap.root)
        if(!_.includes(treemap.loaded_paths, d.path)) {
            if(startLoading()) {
                var q = queueTreemapDataRequests(d);
                awaitTreemapDataRequests(q, d, treemap, load_callback)
            }
        } else {
            console.log("fetchTreemapData d.path=", d.path, " already loaded. treemap.loaded_paths=", treemap.loaded_paths);
        }
    }

    var http_retries = HTTP_RETRY_COUNT;
    fetchTreemapData({path: "/"}, treemap, initialDataLoad);

    function mergeLustreTree(x, y) {
        var merged = x;
        console.log("mergeLustreTree: merging y.path=", y.path, " into merged.path=", merged.path, " merged=", merged);
        var subtree = merged;
        while (subtree.path != y.path) {
            var child_dirs = subtree.child_dirs;
            subtree = _.find(child_dirs, function(child) {
                return ((y.path == child.path) || _.startsWith(y.path, child.path + "/"));
            });

            if(_.isUndefined(subtree)) {
                console.log("mergeLustreTree: cannot find subtree for merge - trees must be disjoint, adding child at at root node (y=", y, " x=", x, " child_dirs=", child_dirs);
                merged.child_dirs.push(y);
                return merged;
            }
        }

        // have subtree to merge, just verify name and path are identical
        if(subtree.name == y.name && subtree.path == y.path) {
            //console.log("mergeLustreTree: have subtree.path=", subtree.path, " subtree=", subtree, " y=", y);

            if(_.isUndefined(subtree["child_dirs"])) { // subtree has no children
                if(!_.isUndefined(y["child_dirs"])) { // y does have children
                    // just go with child_dirs and data from y
                    //console.log("mergeLustreTree: don't have any child_dirs for subtree, returning y[child_dirs]=", y["child_dirs"]);
                    subtree["child_dirs"] = y["child_dirs"];
                    subtree["data"] = y["data"];
                }
            } else { // subtree has children
                if(_.isUndefined(subtree["data"])) { // subtree has no data (this is unexpected)
                    //console.log("mergeLustreTree: don't have any data for subtree, returning y[data]=", y["data"]);
                    subtree["data"] = y["data"];
                } else { // subtree has data
                    if(!_.isUndefined(y["data"])) { // y has data
                        // data needs to be merged
                        //console.log("mergeLustreTree: merging data at subtree.path=", subtree.path, " subtree[data]=", subtree["data"], " and y[data]=", y["data"]);
                        // to merge data, we must have completely disjoint children (otherwise we may be adding data that represents the same children twice)
                        var common_children_paths = _.intersection(_.map(subtree["child_dirs"], path), _.map(y["child_dirs"], path));
                        if(common_children_paths.length == 0) {
                            console.log("have no common child paths between subtree[child_dirs]=", subtree["child_dirs"], " and y[child_dirs]=", y["child_dirs"], " going ahead with data merge for subtree.data=", subtree.data, " y.data=", y.data);
                            subtree["data"] = mergeData(subtree["data"], y["data"]);
                        } else {
                            // just keep subtree["data"] as it is
                            //console.log("mergeLustreTree: refusing to merge data for subtree.path=", path(subtree), " with common_children_paths=", common_children_paths);
                        }
                    }
                }

                //console.log("mergeLustreTree: merging children subtree[child_dirs]=", subtree["child_dirs"], " and y[child_dirs]=", y["child_dirs"]);
                subtree["child_dirs"] = mergeChildren(subtree["child_dirs"], y["child_dirs"]);
            }

            return subtree;
        } else {
            console.log("mergeLustreTree: unexpectedly didn't have equal name and path for subtree=", subtree, " merging y=", y);
            return undefined;
        }
    }

    function mergeData(x, y) {
        //console.log("mergeData: x=", x, " y=", y);
        var merged;
        if(_.isObject(x) && _.isObject(y)) {
            merged = new Object();
            _.forEach(_.union(_.keys(x), _.keys(y)), function(key) {
                //console.log("mergeData recursing to process key=", key);
                merged[key] = mergeData(x[key], y[key]);
            });
        } else if(_.isUndefined(x) && _.isObject(y)) {
            merged = y;
        } else if(_.isUndefined(y) && _.isObject(x)) {
            merged = x;
        } else if(_.isUndefined(x) && !_.isUndefined(y)) {
            merged = +y;
            if(_.isNaN(merged)) {
                console.log("x was undefined, set merged to y but it is NaN. y=", y);
                merged = y;
            }
        } else if(_.isUndefined(y) && !_.isUndefined(x)) {
            merged = +x;
            if(_.isNaN(merged)) {
                console.log("y was undefined, setting merged to x but it is NaN. x=", x);
                merged = x;
            }
        } else if(_.isFinite(+x) && _.isFinite(+y)) {
            merged = +x + +y;
            if(_.isNaN(merged)) {
                console.log("x and y are finite, setting merged to x+y but it is NaN. merged=", merged);
            }
        } else {
            console.log("mergeData: ERROR don't know how to merge x=", x, " y=", y);
            return -1;
        }
        //console.log("mergeData: returning merged=", merged, " for x=", x, " y=", y);
        return merged;
    }

    function mergeChildren(x, y) {
        var merged_children = new Array();
        _.forEach(_.union(_.map(x, path), _.map(y, path)), function(child_path) {
            var x_children = _.filter(x, function(d) {
                return child_path == path(d);
            });
            var num_x_children = _.size(x_children);

            var y_children = _.filter(y, function(d) {
                return child_path == path(d);
            });
            var num_y_children = _.size(y_children);

            if(num_x_children == 1 && num_y_children == 0) {
                //console.log("mergeChildren: using x_children for child_path=", child_path);
                merged_children.push(x_children[0]);
            } else if(num_x_children == 0 && num_y_children == 1) {
                //console.log("mergeChildren: using y_children for child_path=", child_path);
                merged_children.push(y_children[0]);
            } else if(num_x_children == 1 && num_y_children == 1) {
                //console.log("mergeChildren: recursively merging child_path=", child_path);
                var children = mergeLustreTree(x_children[0], y_children[0]);
                //console.log("mergeChildren: children=", children);
                merged_children.push(children);
            } else if(num_x_children == 0 && num_y_children == 0) {
                console.log("mergeChildren: unexpectedly had no children for child_path=", child_path);
            } else if(num_x_children > 1 || num_y_children > 1) {
                console.log("mergeChildren: unexpectedly had more than one child for child_path=", child_path);
            } else {
                console.log("mergeChildren: unexpected error for child_path=", child_path, " x_children=", x_children, " y_children=", y_children);
            }
        });

        //console.log("mergeChildren: returning merged_children=", merged_children);
        return merged_children;
    }

    function bytes_to_human_readable_string(bytes) {
        var base = 1024;
        var labels = BINARY_UNIT_LABELS;

        var unit_index = 0;
        while (unit_index < (labels.length - 1)) {
            var unitmax = Math.pow(base, (unit_index + 1)) * HUMAN_TRIGGER_RATIO;
            if(bytes < unitmax) {
                break;
            }
            unit_index++;
        }

        return (bytes / Math.pow(base, unit_index)).toFixed(1) + OUTPUT_UNIT_SEP + labels[unit_index];
    }

    function count_to_human_readable_string(count) {
        var base = 1000;
        var labels = SI_UNIT_LABELS;

        var unit_index = 0;
        while (unit_index < (labels.length - 1)) {
            var unitmax = Math.pow(base, (unit_index + 1)) * HUMAN_TRIGGER_RATIO;
            if(count < unitmax) {
                break;
            }
            unit_index++;
        }

        return (count / Math.pow(base, unit_index)).toFixed(1) + OUTPUT_UNIT_SEP + labels[unit_index];
    }

    function initialDataLoad() {
        console.log("initialDataLoad: have treemap=", treemap);

        // set title text
//    title.text("Humgen Lustre");

//    _.each(treemap.root.data, function(value, metric){
        // treemap.valueAccessors[key] = function(d) { 
        //     if( _.isObject(d) && _.has(d, key)) { 
        //         return d[key]; 
        //     } else { 
        //         return undefined; 
        //     } 
        // }
        // if(_.isNumber(+value)){ 
        //     var szM = d3.select("#selectSizeMetric").insert("option").attr("value",metric).attr("id","size_metric_"+metric).text(metric);
        //     var flM = d3.select("#selectFillMetric").insert("option").attr("value",metric).attr("id","fill_metric_"+metric).text(metric);
        // }
//    });

        // set selected property on default option
//    d3.select("#size_metric_"+sizeMetric).attr("selected",1);
        //d3.select("#fill_"+fillKey).attr("selected",1);
        initialize_treemap_root(treemap.root);
        layout(treemap.root);

        // set size and color options
        var metric_keys = _.map(treemap.root.data, function(v, k) {
            return k;
        });
        console.log("initialDataLoad: metric_keys=", metric_keys);

        var szM = d3.select("#select_size_metric_form").selectAll("span")
            .data(metric_keys, function(d) {
                return d;
            });
        szM.exit().remove();
        szM.enter().append("span")
            .attr("id", function(d) {
                return "size_metric_span_" + d;
            });

        var szMinput = szM.append("input")
            .attr("type", "radio")
            .attr("name", "size_metric")
            .attr("value", function(d) {
                return d;
            })
            .attr("id", function(d) {
                return "size_metric_" + d;
            });

        szMinput.filter(function(d) {
            return d == treemap.size.metric;
        })
            .property("checked", true);

        var szMlabel = szM.append("span")
            .text(displayMetric);

        szMlabel.append("br");

        var flM = d3.select("#select_fill_metric_form").selectAll("span")
            .data(metric_keys, function(d) {
                return d;
            });
        flM.exit().remove();
        flM.enter().append("span")
            .attr("id", function(d) {
                return "fill_metric_span_" + d;
            });

        var flMinput = flM.append("input")
            .attr("type", "radio")
            .attr("name", "fill_metric")
            .attr("value", function(d) {
                return d;
            })
            .attr("id", function(d) {
                return "fill_metric_" + d;
            });

        flMinput.filter(function(d) {
            return d == treemap.fill.metric;
        })
            .property("checked", true);

        var flMlabel = flM.append("span")
            .text(displayMetric);

        flMlabel.append("br");

        // set group/user/tag according to those in treemap.root
        setFilterOptions(treemap.root, treemap.size, "size");
        setFilterOptions(treemap.root, treemap.fill, "fill");

        // have to generate initial color scale after layout, as before that treemap.nodes is unset
        treemap.color = treemap.genColorScale(treemap.root, treemap.fillAccessor);
        console.log("treemap.color generator set to: ", treemap.color);

        curg = display(treemap.root);
        if(_.isUndefined(node)) {
            console.log("setting node to treemap.root");
            node = treemap.root; // TODO still needed?
        }
    }

    function setSelectOptions(node, filters, property, select_name, select_options) {
        var select_form = d3.select("#select_" + property + "_" + select_name + "_form");
        var select_opts = select_form.selectAll("option")
            .data(select_options, function(d) {
                return d;
            });
        select_opts.exit().remove();
        select_opts.enter().append("option")
            .attr("id", function(d) {
                return property + "_group_" + d;
            })
            .attr("name", property + "_group")
            .attr("value", function(d) {
                return d;
            })
            .text(function(d) {
                return d;
            });

        select_opts.filter(function(d) {
            return d == filters[select_name];
        })
            .property("selected", true);

        select_form.on("change", function() {
            //console.log("changed filter property=", property, " select_name=", select_name, " this.value=", this.value);
            treemap[property][select_name] = this.value;
            if(property == "size") {
                treemap.sizeAccessor = getValueAccessor(treemap[property].metric, treemap[property]);
            } else if(property == "fill") {
                treemap.fillAccessor = getValueAccessor(treemap[property].metric, treemap[property]);
            }
            setFilterOptions(node, treemap[property], property);
            layout(node);
            transition(node);
        });

    }

    function setFilterOptions(node, filters, property) {
        var groups = getGroupList(node, filters);
        setSelectOptions(node, filters, property, "group", groups);

        var users = getUserList(node, filters);
        setSelectOptions(node, filters, property, "user", users);

        var tags = getTagList(node, filters);
        setSelectOptions(node, filters, property, "tag", tags);
    }

    function getGroupList(d, filters) {
        return _.keys(d.data[filters.metric]).sort();
    }

    function getUserList(d, filters) {
        return _.keys(d.data[filters.metric][filters.group]).sort();
    }

    function getTagList(d, filters) {
        return _.keys(d.data[filters.metric][filters.group][filters.user]).sort();
    }

    function initialize_treemap_root(root) {
        root.x = root.y = 0;
        root.dx = width;
        root.dy = height;
        root.depth = 0;
    }

    // Compute the treemap layout recursively such that each group of siblings
    // uses the same size (1×1) rather than the dimensions of the parent cell.
    // This optimizes the layout for the current zoom state. Note that a wrapper
    // object is created for the parent node for each group of siblings so that
    // the parent’s dimensions are not discarded as we recurse. Since each group
    // of sibling was laid out in 1×1, we must rescale to fit using absolute
    // coordinates. This lets us use a viewport to zoom.
    function layout(d, depth) {
//    console.log("layout: d=", d, " depth=", depth);
        if(_.isUndefined(depth)) {
            depth = 0;
        }
        if(depth == 0) {
            treemap.layout.value(treemap.sizeAccessor);
            treemap.nodes = [];
        }

        if(d.child_dirs && depth < 3) {
            //treemap.layout.size([d.width, d.height]);
            var childNodes = treemap.layout.nodes({data: d.data, child_dirs: d.child_dirs});
            treemap.nodes.push(childNodes);
            d.child_dirs.forEach(function(c) {
                c.x = d.x + c.x * d.dx;
                c.y = d.y + c.y * d.dy;
                c.dx *= d.dx;
                c.dy *= d.dy;
                c.parent = d;
                layout(c, depth + 1);
            });
        }
        treemap.nodes = _.flatten(treemap.nodes);

//    console.log("layout: calling treemap.layout.nodes on d=", d);
//    treemap.nodes = treemap.layout.nodes(d);
//    console.log("layout: have treemap.nodes=", treemap.nodes);
//    console.log("layout: after layout, d=", d);

//    treemap.nodes = treemap.layout.nodes(d);
//console.log("treemap.nodes=", treemap.nodes);
        // d.child_dirs.forEach(function(c) {
        //     c.x = d.x + c.x * d.dx;
        //     c.y = d.y + c.y * d.dy;
        //     c.dx *= d.dx;
        //     c.dy *= d.dy;
        //     c.parent = d;
        //     //treemap.layout.nodes(c);
        //     //layout(c); // TODO is this required?
        // });
//    treemap.nodes = _.flatten(treemap.nodes);

        //console.log("layout: returning treemap.nodes=", treemap.nodes);
        return treemap.nodes;
    }

    function display(d) {
        console.log("display: d=", d);
        treemap.grandparent
            .datum(d.parent)
            .on("click", function(d) {
                if(!_.isUndefined(d)) {
                    // regenerate color scale based on new layout
                    treemap.color = treemap.genColorScale(d, treemap.fillAccessor);

                    setFilterOptions(d, treemap.size, "size");
                    setFilterOptions(d, treemap.fill, "fill");

                    //console.log("clicked grandparent=", d);
                    //console.log("grandparent clicked d.path="+d.path);
                    transition(d, 250);
                } else {
                    console.log("grandparent undefined");
                }
            })
            .select("text")
            .text(path(d));
        //minmax = calcMinMax(treemap.nodes, treemap.fillAccessor);

        var g1 = treemap.svg.insert("g", ".grandparent")
            .datum(d)
            .classed("depth", true)
            .attr("id", depthId);

        //console.log("creating tooltips for d.child_dirs=", d.child_dirs);
        var div = d3.select("#footer").selectAll("div.tooltip")
            .data(d.child_dirs, path);
        div.exit().remove();
        div.enter().append("div")
            .classed("tooltip", true)
            .attr("id", tooltipId)
            .style("opacity", 1e-6);

        //console.log("creating group element for d.child_dirs=", d.child_dirs);
        var parent_and_children = g1.selectAll("g.parent_and_children")
            .data(d.child_dirs)
            .enter().append("svg:g");

        parent_and_children
            .classed("parent_and_children", true)
            .on("mouseover", mouseover)
            .on("mouseout", mouseout);

        parent_and_children
            .on("click", function(child) {
                console.log("clicked child=", child);
                if(isLoading()) {
                    console.log("already loading treemap data, refusing to respond to this click");
                    //TODO pop-up a warning to user that they need to wait!
                    alert("Data load in progress, please try again in a moment!");
                    return;
                }
                if(_.size(child.child_dirs) > 0) {
                    fetchTreemapData(child, treemap, function() {
                        console.log("new data loaded for child=", child);
                        layout(child);
                    });

                    // regenerate color scale based on new layout
                    treemap.color = treemap.genColorScale(child, treemap.fillAccessor);

                    setFilterOptions(child, treemap.size, "size");
                    setFilterOptions(child, treemap.fill, "fill");

                    // console.log("before transition, treemap.root=", treemap.root)
                    transition(child, 750);
//            console.log("after transition, treemap.root=", treemap.root)
                } else {
                    console.log("no children for child=", child, " - not transitioning");
                }
            });

        function tooltipText(d) {
            // todo move template generation out
            var text_template = _.template("<table><caption><%- path %></caption><tr><th>Metric</th><th>Total</th><% if(area_filter_label != '') { %><th>Filtered: <%- area_filter_label %></th><% } %><% if(fill_filter_label != '') { %><th>Filtered: <%- fill_filter_label %></th><% } %></tr><% _.forEach(labels, function(label) { %><tr><td><%- label.key %></td><td><%- label.value %></td><% if(area_filter_label != '') { %><td><%- label.areavalue %></td><% } %><% if(fill_filter_label != '') { %><td><%- label.fillvalue %></td><% } %></tr><% }); %></table>");
            //        var text_template = _.template("Path: <%= path %>");
            var text_items = ["size", "count", "ctime", "atime", "mtime"]; //, sizeKey, fillKey];
            var text_data = {
                "path": displayValue(d, "path"),
                "area_filter_label": displayFilters(treemap.size),
                "fill_filter_label": displayFilters(treemap.fill),
                "labels": _.map(text_items, function(item) {
                    //console.log("for item: ", item, " have key:", displayKey(item));
                    return {
                        key: displayKey(item),
                        value: displayValue(d, item),
                        areavalue: displayValue(d, item, treemap.size),
                        fillvalue: displayValue(d, item, treemap.fill)
                    };
                })
            };
            var text = text_template(text_data);
            //console.log("generated tooltiptext for d:", d, " text: ", text);

            return text;
        }

        function mouseover(g) {
            //console.log("mouseover! path="+g.path);
            d3.selectAll("div.tooltip").filter(function(d) {
                return d.path != g.path;
            })
                .html(tooltipText)
                .transition()
                .duration(500)
                .style("opacity", 1e-6);
            d3.selectAll("div.tooltip").filter(function(d) {
                return d.path == g.path;
            })
                .html(tooltipText)
                .transition()
                .duration(500)
                .style("opacity", 1);
        }

        function mouseout() {
            var div = d3.selectAll("div.tooltip");
            div.transition()
                .duration(500)
                .style("opacity", 1e-6);
        }

        console.log("creating child rect for parent_and_children=", parent_and_children);
        parent_and_children.selectAll(".child")
            .data(function(d) {
                return d.child_dirs || [d];
            })
            .enter().append("rect")
            .classed("child", true)
            .call(treebox)
            .style("fill", fillColor);

        parent_and_children.append("rect")
            .classed("parent", true)
            .call(treebox)
            .style("fill", fillColor);

        var titlesvg = parent_and_children.append("svg")
            .classed("parent_title", true)
            .attr("viewBox", "-100 -10 200 20")
            .attr("preserveAspectRatio", "xMidYMid meet")
            .call(treebox);

        titlesvg.append("text")
            .attr("font-size", 16)
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", 200)
            .attr("height", 20)
            .attr("dy", ".3em")
            .style("text-anchor", "middle")
            .text(function(d) {
                return name(d);
            });

        d3.selectAll("#select_size_metric_form input").on("change", function() {
            treemap.size.metric = this.value;
            treemap.sizeAccessor = getValueAccessor(treemap.size.metric, treemap.size);
            console.log("treemap.size.metric changed to " + treemap.size.metric + " treemap.sizeAccessor now ", treemap.sizeAccessor);
            setFilterOptions(node, treemap.size, "size");
            layout(node);
            transition(node, 750);
        });

        d3.selectAll("#select_fill_metric_form input").on("change", function() {
            treemap.fill.metric = this.value;
            treemap.fillAccessor = getValueAccessor(treemap.fill.metric, treemap.fill);
            treemap.color = treemap.genColorScale(node, treemap.fillAccessor);
            console.log("treemap.fill.metric changed to " + treemap.fill.metric + " treemap.fillAccessor now ", treemap.fillAccessor);
            setFilterOptions(node, treemap.fill, "fill");
            layout(node);
            transition(node, 750);
        });
        return g1;
    }


    function transition(d, time) {
        //        console.log("transition!");
        if(_.isUndefined(time)) {
            time = 750;
        }
        node = d;
        if(transitioning || !d) {
            console.log("already transitioning, refusing to cut to new transition");
            return;
        }
        transitioning = true;

        var g2 = display(d),
            t1 = curg.transition().duration(time),
            t2 = g2.transition().duration(time);

        // Update the domain only after entering new elements.
        x.domain([d.x, d.x + d.dx]);
        y.domain([d.y, d.y + d.dy]);

        // Enable anti-aliasing during the transition.
        treemap.svg.style("shape-rendering", null);

        // Draw child nodes on top of parent nodes.
        treemap.svg.selectAll(".depth").sort(function(a, b) {
            return a.depth - b.depth;
        });

        // Fade-in entering text.
        g2.selectAll("text").style("fill-opacity", 0);

        // Transition to the new view.
        t1.selectAll(".parent_title").call(treebox);
        t2.selectAll(".parent_title").call(treebox);
        t1.selectAll("text").style("fill-opacity", 0);
        t2.selectAll("text").style("fill-opacity", 1);
        t1.selectAll("rect").call(treebox).style("fill", fillColor);
        t2.selectAll("rect").call(treebox).style("fill", fillColor);

        // Remove the old node when the transition is finished.
        t1.remove().each("end", function(d) {
            treemap.svg.style("shape-rendering", "crispEdges");
            transitioning = false;
            curg = g2;
        });

    }

    function textbox(text) {
        text.attr("x", function(d) {
            return x(d.x) + 6;
        })
            .attr("y", function(d) {
                return y(d.y) + 6;
            });
    }

    function treebox(b) {
        b.attr("x", function(d) {
            //console.log("treebox: d.x=", d.x, " x(d.x)=", x(d.x), " d=", d);
            return x(d.x);
        })
            .attr("y", function(d) {
                return y(d.y);
            })
            .attr("width", function(d) {
                return x(d.x + d.dx) - x(d.x);
            })
            .attr("height", function(d) {
                return y(d.y + d.dy) - y(d.y);
            });
    }

    function name(d) {
        return d.name;
    }

    function path(d) {
        return d.path;
    }

    function tooltipId(d) {
        return "tooltip:" + path(d);
    }

    function depthId(d) {
        return "depth:" + path(d);
    }


    return treemap;
}); // treemap module

