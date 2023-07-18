import useURLState, { useClearState } from "./State";
import { useSearchParams } from "react-router-dom";
import MultiSelect from "./MultiSelect";
import { useEffect } from 'react';

function useClearParams() {
    useEffect(() => {
        if (!window.location.search) return;
        window.history.replaceState(null, '', window.location.origin);
    }, []);
}

export default function Filter({ users }: { users: string[] }) {
    const [searchParams, setSearchParams] = useSearchParams();
    const [byUser, setByUser] = useURLState<boolean>("byUser", false)
    const [selectedUsers, setSelectedUsers] = useURLState<string[]>("users", [])

    function clear(item: [string, string], index, arr) {
        console.log("delete item %s: %s", item[0], item[1])
        searchParams.delete(item[0])
    }

    const onByGroupChange = (e) => {
        window.history.pushState(Date.now(), "", "?");
        [...searchParams.entries()].forEach(clear);
        setSearchParams(searchParams);
        setByUser(!e.target.checked);
    }

    const onByUserChange = (e) => {
        window.history.pushState(Date.now(), "", "?");
        [...searchParams.entries()].forEach(clear);
        setSearchParams(searchParams);
        setByUser(e.target.checked);
    }

    return <div className="treeFilter">
        <label htmlFor="byGroup">By Group</label>
        <input type="radio" name="by" id="byGroup" checked={!byUser} onChange={onByGroupChange} />

        <label htmlFor="byUser">By User</label>
        <input type="radio" name="by" id="byUser" checked={byUser} onChange={onByUserChange} />

        <label htmlFor="username">Username</label>
        <MultiSelect id="username" list={users} selected={selectedUsers} setSelected={setSelectedUsers} />

        {/* <label htmlFor="unix">Unix Group</label>
        <MultiSelect id="unix" list={Array.from(new Set(groupUsage.map(e => e.Name)).values()).sort(stringSort)} listener={(cb: Listener) => groupPipe = cb} onchange={groups => setGroups(groups.map(groupname => groupNameToIDMap.get(groupname) ?? -1))} />
        <label htmlFor="bom">Group Areas</label>
        <MultiSelect id="bom" list={Object.keys(areas).sort(stringSort)} selectedList={selectedBOMs} onchange={(boms, deleted) => {
            if (deleted) {
                groupPipe?.(areas[deleted], true);

                return false;
            }

            const existingGroups = new Set(groups.map(gid => groupIDToNameMap.get(gid) ?? ""));

            for (const bom of boms) {
                for (const g of areas[bom] ?? []) {
                    if (groupNameToIDMap.has(g)) {
                        existingGroups.add(g);
                    }
                }
            }

            groupPipe?.(Array.from(existingGroups), false);

            return false;
        }} />
        <label htmlFor="owners">Owners</label>
        <MultiSelect id="owners" list={Array.from(new Set(groupUsage.map(e => e.Owner).filter(o => o)).values()).sort(stringSort)} onchange={setOwners} disabled={byUser} />
        <label>Size </label>
        <Minmax max={userUsage.concat(groupUsage).map(u => u.UsageSize).reduce((max, curr) => Math.max(max, curr), 0)} width={sliderWidth} minValue={filterMinSize} maxValue={filterMaxSize} onchange={(min: number, max: number) => {
            setFilterMinSize(min);
            setFilterMaxSize(max);
        }} formatter={formatBytes} />
        <label>Last Modified</label>
        <Minmax max={userUsage.concat(groupUsage).map(e => asDaysAgo(e.Mtime)).reduce((curr, next) => Math.max(curr, next), 0)} minValue={filterMinDaysAgo} maxValue={filterMaxDaysAgo} width={sliderWidth} onchange={(min: number, max: number) => {
            setFilterMinDaysAgo(min);
            setFilterMaxDaysAgo(max);
        }} formatter={formatNumber} />
        <label htmlFor="scaleSize">Log Size Axis</label>
        <input type="checkbox" id="scaleSize" checked={scaleSize} onChange={e => setScaleSize(e.target.checked)} />
        <label htmlFor="scaleDays">Log Days Axis</label>
        <input type="checkbox" id="scaleDays" checked={scaleDays} onChange={e => setScaleDays(e.target.checked)} />
        <button onClick={clearState}>Reset Filter</button> */}
    </div>
}