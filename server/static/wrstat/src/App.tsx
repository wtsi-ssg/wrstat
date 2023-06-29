import type {Usage} from './rpc';
import Filter from './filter';

const logout = () => {
  const expireNow = "=;expires=Thu, 1 Jan 1970 00:00:00 GMT;path=/";
  
  for (const cookie of document.cookie.split("; ")) {
    document.cookie = cookie.split("=")[0] + expireNow;
  }

  window.location.reload();
};

export default ({username, groupUsage, userUsage /*, history*/}: {username: string, groupUsage: Usage[], userUsage: Usage[] /*, history: Map<string, History[]>*/}) => {
  if (!username) {
    return (
      <div><form action="/login"><input type="submit" value="Login" /></form></div>
    );
  }

  return (
    <div>
      <div id="auth">{username} - <button onClick={logout}>Logout</button></div>
      <Filter groupUsage={groupUsage} userUsage={userUsage} /*history={history}*/ />
    </div>
  );
};