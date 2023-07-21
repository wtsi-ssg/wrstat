import type { MouseEventHandler } from "react";

type PaginationParams = {
	totalPages: number;
	currentPage: number;
	onClick: MouseEventHandler;
}

const paginationEnd = 3,
	paginationSurround = 3,
	noop = () => { },
	processPaginationSection = (ret: JSX.Element[], currPage: number, from: number, to: number, onClick: MouseEventHandler) => {
		if (ret.length !== 0) {
			ret.push(<li key={`pagination_gap_${from}`}>…</li>);
		}

		for (let p = from; p <= to; p++) {
			ret.push(<li
				key={`pagination_page_${p}`}
				tabIndex={0}
				role={currPage === p ? undefined : "button"}
				aria-label={currPage === p ? "Current Table Page" : `Go to Table Page ${p + 1}`}
				aria-current={currPage === p ? "page" : undefined}
				className={currPage === p ? "pagination_selected" : "pagination_link"}
				onClick={currPage === p ? noop : onClick}
				data-page={p}
			>{p + 1}</li>);
		}
	},
	PaginationComponent = ({ totalPages, currentPage, onClick }: PaginationParams) => {
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
				(paginationEnd > 0 && ((currentPage - paginationSurround - 1 === paginationEnd && page === paginationEnd) ||
					(currentPage + paginationSurround + 1 === lastPage - paginationEnd && page === lastPage - paginationEnd))))) {
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
			<li
				key={`pagination_prev`}
				tabIndex={0}
				aria-label={`Go to Previous Table Page`}
				className={"pagination_prev" + (currentPage === 0 ? "" : " pagination_link")}
				onClick={currentPage === 0 ? noop : onClick}
				data-page={currentPage - 1}
			>Previous</li>
			{ret}
			<li
				key={`pagination_next`}
				tabIndex={0}
				aria-label={`Go to Next Table Page`}
				className={"pagination_next" + (currentPage === lastPage ? "" : " pagination_link")}
				onClick={currentPage === lastPage ? noop : onClick}
				data-page={currentPage + 1}
			>Next</li>
		</ul >
	};

export default PaginationComponent;