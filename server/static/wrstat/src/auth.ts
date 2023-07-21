const usernameFromJWT = (jwt: string): string => JSON.parse(atob(jwt.split('.')[1])).Username,
	getJWT = () => new Promise<string>((successFn, errorFn) => {
		const xh = new XMLHttpRequest();

		xh.addEventListener("readystatechange", () => {
			if (xh.readyState === 4) {
				if (xh.status === 200) {
					const token = JSON.parse(xh.response);

					document.cookie = `jwt=${token};samesite=strict;path=/tree`

					successFn(usernameFromJWT(token));
				} else {
					errorFn(new Error(xh.responseText));
				}
			}
		});
		xh.open("POST", "/rest/v1/jwt");
		xh.send(null);
	});

export const getCookie = (toFind: string) => {
	for (const cookie of document.cookie.split("; ")) {
		const [name, value] = cookie.split("=");

		if (name === toFind) {
			return value;
		}
	}

	return "";
},
	logout = () => {
		document.cookie = "jwt=;expires=Thu, 1 Jan 1970 00:00:00 GMT;path=/tree";
		document.cookie = "okta-hosted-login-session-store=;expires=Thu, 1 Jan 1970 00:00:00 GMT;path=/";

		window.location.reload();
	};

const AuthComponent = () => {
	const jwt = getCookie("jwt"),
		oidc = getCookie("okta-hosted-login-session-store");

	if (jwt) {
		return Promise.resolve(usernameFromJWT(jwt));
	}

	if (!oidc) {
		return Promise.reject();
	}

	return getJWT();
};

export default AuthComponent;