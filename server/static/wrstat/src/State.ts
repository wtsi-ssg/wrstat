import { useSearchParams } from "react-router-dom";

export default function useURLState<T>(
    searchParamName: string,
    defaultValue: T
): readonly [
    searchParamsState: T,
    setSearchParamsState: (newState: T) => void
] {
    const [searchParams, setSearchParams] = useSearchParams();

    const acquiredSearchParamStr = searchParams.get(searchParamName);
    let searchParamsState = defaultValue
    if (acquiredSearchParamStr !== null) {
        let d = decodeURIComponent(acquiredSearchParamStr);
        console.log('%s => %s', acquiredSearchParamStr, d);
        searchParamsState = JSON.parse(d) as T
    }

    const setSearchParamsState = (newState: T) => {
        const serialized = encodeURIComponent(JSON.stringify(newState))
        console.log('serialzed key %s val %s to %s', searchParamName, newState, serialized)
        const next = Object.assign(
            {},
            [...searchParams.entries()].reduce(
                (o, [key, value]) => ({ ...o, [key]: value }),
                {}
            ),
            { [searchParamName]: serialized }
        );
        setSearchParams(next);
        console.log('setSerachParams(%s)', JSON.stringify(next))
    };
    return [searchParamsState, setSearchParamsState];
}

export function useClearState() {
    const [_, setSearchParams] = useSearchParams();
    setSearchParams(undefined)
}