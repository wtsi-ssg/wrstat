import type { Usage } from "./rpc";
import Minmax from "./MinMax";
import { asDaysAgo, formatBytes, formatNumber } from "./format";
import GroupUserFilter, { type GroupUserFilterParams } from "./GroupUserFilter";
import { clearState } from "./state";

type FilterParams = {
	usage: Usage[];
	axisMinSize: number;
	setAxisMinSize: (v: number) => void;
	axisMaxSize: number;
	setAxisMaxSize: (v: number) => void;
	axisMinDaysAgo: number;
	setAxisMinDaysAgo: (v: number) => void;
	axisMaxDaysAgo: number;
	setAxisMaxDaysAgo: (v: number) => void;
	scaleSize: boolean;
	setScaleSize: (v: boolean) => void;
	scaleDays: boolean;
	setScaleDays: (v: boolean) => void;
	guf: GroupUserFilterParams;
	setByUser: (v: boolean) => void;
}

const FilterComponent = ({
	usage,
	axisMinSize, setAxisMinSize,
	axisMaxSize, setAxisMaxSize,
	axisMinDaysAgo, setAxisMinDaysAgo,
	axisMaxDaysAgo, setAxisMaxDaysAgo,
	scaleSize, setScaleSize,
	scaleDays, setScaleDays,
	guf,
	setByUser
}: FilterParams) => {
	return <>
		<div className="treeFilter">
			<GroupUserFilter {...guf} num={0} />
			<label>Size</label>
			<Minmax
				max={usage.reduce((max, curr) => Math.max(max, curr.UsageSize), 0)}
				width={300}
				minValue={axisMinSize}
				maxValue={axisMaxSize}
				onchange={(min: number, max: number) => {
					setAxisMinSize(min);
					setAxisMaxSize(max);
				}}
				formatter={formatBytes}
				label="Size Filter" />
			<label>Last Modified</label>
			<Minmax
				max={usage.reduce((curr, next) => Math.max(curr, asDaysAgo(next.Mtime)), 0)}
				minValue={axisMinDaysAgo}
				maxValue={axisMaxDaysAgo}
				width={300}
				onchange={(min: number, max: number) => {
					setAxisMinDaysAgo(min);
					setAxisMaxDaysAgo(max);
				}}
				formatter={formatNumber}
				label="Days since last modified" />
			<label htmlFor="scaleSize">Log Size Axis</label>
			<input type="checkbox" id="scaleSize" checked={scaleSize} onChange={e => setScaleSize(e.target.checked)} />
			<label htmlFor="scaleDays">Log Days Axis</label>
			<input type="checkbox" id="scaleDays" checked={scaleDays} onChange={e => setScaleDays(e.target.checked)} />
			<button onClick={() => {
				clearState();
				setByUser(guf.byUser);
			}}>Reset</button>
		</div>
	</>
};

export default FilterComponent;