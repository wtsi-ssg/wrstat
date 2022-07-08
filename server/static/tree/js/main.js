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
        $("#login").hide()
        $("#body").show()

        require(["dtreemap"], function () {
            console.log("treemap module loaded");
        });
    }

    $(document).ready(function () {
        jwt = cookie.get('jwt')

        if (jwt && jwt.length !== 0) {
            showMap(jwt)
            return
        } 

        let oidc = cookie.get("okta-hosted-login-session-store");
        if (oidc && oidc.length !== 0) {
            fetch("/oidc-jwt", {
                method: "POST", 
                body: JSON.stringify({
                    token: oidc
                })
            })
            .then(d => d.json())
            .then((token) => {
                cookie.set("jwt", token, {path: "", secure: true, sameSite: "strict"});
                showMap(token);
            })
                
        } else {
            $("#login").show()
        }
    });

    $("#loginForm").submit(function (event) {
        event.preventDefault();

        var posting = $.post("/rest/v1/jwt", $("#loginForm").serialize(), "json");

        posting.done(function (data) {
            cookie.set('jwt', data, { expires: 7, path: '', secure: true, sameSite: 'strict' })

            showMap(data)
        });

        posting.fail(function () {
            $("#loginFailure").empty().append("Incorrect username or password");
            $("#body").hide()
        });
    });
});
