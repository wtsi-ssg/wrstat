import type { MouseEventHandler } from "react";

const paginationEnd = 3,
	paginationSurround = 3,
	noop = () => { },
	processPaginationSection = (ret: JSX.Element[], currPage: number, from: number, to: number, onClick: MouseEventHandler) => {
		if (ret.length !== 0) {
			ret.push(<li>…</li>);
		}

		for (let p = from; p <= to; p++) {
			ret.push(<li className={currPage === p ? "pagination_selected" : "pagination_link"} onClick={currPage === p ? noop : onClick} data-page={p}>{p + 1}</li>);
		}
	};

export default ({ totalPages, currentPage, onClick }: { totalPages: number; currentPage: number, onClick: MouseEventHandler }) => {
	const ret: JSX.Element[] = [],
		lastPage = totalPages - 1;
	if (lastPage < 1) {
		return <></>
	}

	if (currentPage > lastPage) {
		currentPage = lastPage;
	}

	let start = 0;

	for (let page = 0; page <= lastPage; page++) {
		if (!(page < paginationEnd || page > lastPage - paginationEnd ||
			((paginationSurround > currentPage ||
				page >= currentPage - paginationSurround) && page <= currentPage + paginationSurround) ||
			paginationEnd > 0 && ((currentPage - paginationSurround - 1 === paginationEnd && page === paginationEnd) ||
				(currentPage + paginationSurround + 1 === lastPage - paginationEnd && page === lastPage - paginationEnd)))) {
			if (page !== start) {
				processPaginationSection(ret, currentPage, start, page - 1, onClick);
			}
			start = page + 1
		}
	}

	if (start < lastPage) {
		processPaginationSection(ret, currentPage, start, lastPage, onClick);
	}

	return <ul className="pagination">
		<li className={"pagination_prev" + (currentPage === 0 ? "" : " pagination_link")} onClick={currentPage === 0 ? noop : onClick} data-page={currentPage - 1}>Previous</li>
		{ret}
		<li className={"pagination_next" + (currentPage === lastPage ? "" : " pagination_link")} onClick={currentPage === lastPage ? noop : onClick} data-page={currentPage + 1}>Next</li>
	</ul>
};