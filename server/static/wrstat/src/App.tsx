import Filter from './Filter';

export default function App({ users }: { users: string[] }) {
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