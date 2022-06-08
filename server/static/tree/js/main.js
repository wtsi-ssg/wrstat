require.config({
    paths: {
        d3: "d3.v3.min",
        queue: "queue.v1",
        lodash: "lodash.min",
        jquery: "jquery-3.6.0.min",
        cookie: "js.cookie.min"
    }
});

requirejs(['jquery', 'cookie'], function ($, cookie) {
    function showMap(jwt) {
        header = "Bearer " + jwt
        $.ajaxSetup({
            headers: {
                'Authorization': header
            }
        })

        $("#login").hide()
        $("#body").show()

        var got = $.ajax({
            type: 'GET',
            url: '/rest/v1/auth/tree?path=/',
            dataType: 'json',
        });

        got.done(function (data) {
            console.log("worked with ajax")
            console.log(data)
        });

        got.fail(function (data) {
            console.log("failed")
            console.log(data)
        });

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
            console.log(data)
            cookie.set('jwt', data, { expires: 7, path: '', secure: true, sameSite: 'strict' })

            showMap(data)
        });

        posting.fail(function () {
            $("#loginFailure").empty().append("Incorrect username or password");
            $("#body").hide()
        });
    });
});
