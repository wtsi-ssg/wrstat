require.config({
    paths: {
        d3: "d3.v3.min",
        queue: "queue.v1",
        lodash: "lodash.min",
        jquery: "jquery-3.6.0.min",
        cookie: "js.cookie.min",
        flexdatalist: "jquery.flexdatalist.min"
    }
});

requirejs.config({
    shim: {
        'flexdatalist': {
            deps: ['jquery']
        },
    }
});

const getUsernameFromJWT = (token) => {
    return JSON.parse(atob(token.split('.')[1])).Username;
};

requirejs(['jquery', 'cookie', 'flexdatalist'], function ($, cookie) {
    function showMap(jwt) {
        $("#login").hide()
        $("#body").show()
        $("#username").text(getUsernameFromJWT(jwt));

        require(["dtreemap"], function () {
            console.log("treemap module loaded");
        });
    }

    $(document).ready(function () {
        jwt = cookie.get('jwt')

        if (jwt && jwt.length !== 0) {
            showMap(jwt)
        } else {
            $("#login").show()
        }
    });

    $("#loginForm").submit(function (event) {
        event.preventDefault();

        var posting = $.post("/rest/v1/jwt", $("#loginForm").serialize(), "json");

        posting.done(function (data) {
            cookie.set('jwt', data, { path: '', secure: true, sameSite: 'strict' })

            showMap(data)
        });

        posting.fail(function () {
            $("#loginFailure").empty().append("Incorrect username or password");
            $("#body").hide()
        });
    });

    $("#logoutButton").click(() => {
        cookie.remove("jwt", { path: "" });
        window.location.reload();
    });
});
