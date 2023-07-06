const usernameFromJWT = (jwt: string): string => {
	return JSON.parse(atob(jwt.split('.')[1])).Username;
};

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
	const expireNow = "=;expires=Thu, 1 Jan 1970 00:00:00 GMT;path=/";

	for (const cookie of document.cookie.split("; ")) {
	  document.cookie = cookie.split("=")[0] + expireNow;
	}

	window.location.reload();
};

export default () => {
	const jwt = getCookie("jwt");

	if (jwt) {
		return Promise.resolve(usernameFromJWT(jwt));
	}

	const oidc = getCookie("okta-hosted-login-session-store");
	if (!oidc) {
		return Promise.reject();
	}

	return new Promise<string>((successFn, errorFn) => {
		const xh = new XMLHttpRequest();

		xh.addEventListener("readystatechange", () => {
			if (xh.readyState === 4) {
				if (xh.status === 200) {
					const token = JSON.parse(xh.response);

					document.cookie = `jwt=${token};samesite=strict;path=/`

					successFn(usernameFromJWT(token));
				} else {
					errorFn(new Error(xh.responseText));
				}
			}
		});
		xh.open("POST", "/rest/v1/jwt");
		xh.send(null);
	});
};