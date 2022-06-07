require.config({
    paths: {
        d3: "d3.v3",
        queue: "queue.v1",
        lodash: "lodash"
    }
});

require(["treemap"],
    function(treemap) {
        console.log("treemap module loaded: ", treemap);
    }
);
