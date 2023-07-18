import Filter from './Filter';

const stringSort = new Intl.Collator().compare

export default function App({ groupUsage, userUsage }: { groupUsage: Usage[], userUsage: Usage[] }) {
    const users = Array.from(new Set(userUsage.map(e => e.Name)).values()).sort(stringSort)

    return <>
        <details open className="boxed">
            <summary>Filter</summary>
            <div className="primaryFilter">
                <Filter users={users} />
            </div>
        </details>
    </>
}

// <Filter groupUsage={groupUsage} userUsage={userUsage} areas={areas} />