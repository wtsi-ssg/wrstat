type TabData = {
	title: string;
	onClick: (title: string) => void;
	selected?: boolean;
}

const Tabs = ({ id, tabs }: { id: string, tabs: TabData[] }) => <ul id={id} className="tabs">
	{tabs.map(tab => <li className={tab.selected ? "selected" : ""} onClick={tab.selected ? undefined : () => tab.onClick(tab.title)}>{tab.title}</li>)}
</ul>

export default Tabs;