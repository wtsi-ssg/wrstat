type TabData = {
	title: string;
	onClick: (title: string) => void;
	selected?: boolean;
}

const Tabs = ({ id, tabs }: { id: string, tabs: TabData[] }) => <ul id={id} className="tabs">
	{tabs.map((tab, n) => <li
		tabIndex={0}
		key={`tab_${id}_${n}`}
		role="tab"
		aria-selected={tab.selected ? "true" : "false"}
		className={tab.selected ? "selected" : ""}
		onClick={tab.selected ? undefined : e => {
			if (e.button !== 0) {
				return;
			}

			tab.onClick(tab.title);
		}}
		onKeyDown={tab.selected ? undefined : e => {
			if (e.key === "Enter") {
				tab.onClick(tab.title);
			}
		}}
	>{tab.title}</li>)}
</ul>

export default Tabs;