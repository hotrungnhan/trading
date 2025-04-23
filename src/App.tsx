import { bind, Subscribe } from "@react-rxjs/core";
import { createSignal } from "@react-rxjs/utils";
import { useCallback } from "react";
import {
	debounceTime,
	Observable,
	merge,
	scheduled,
	asyncScheduler,
	combineLatestAll,
	map,
} from "rxjs";
import { ToastContainer, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";
import classNames from "classnames";

const fields = [
	{
		name: "account-balance",
		title: "Account Balance USD:",
		defaultValue: 1000,
		type: "number",
		required: true,
		lockable: false,
	},
	{
		name: "risk-percent",
		title: "Risk (%) :",
		defaultValue: 2,
		type: "number",
		step: 0.01,
		min: 0,
		max: 100,
		required: true,
		lockable: true,
	},
	// {
	//   name: "risk-usd",
	//   title: "Risk USD:",
	//   defaultValue: "",
	//   type: "number",
	// },
	{
		name: "entry-price",
		title: "Entry Price (USD):",
		type: "number",
		required: true,
	},
	{
		name: "entry-fee",
		title: "Entry Fee (%):",
		defaultValue: 0.01,
		type: "number",
		min: 0,
		max: 100,
		lockable: true,
	},
	{
		name: "stoploss",
		title: "Stoploss (USD):",
		type: "number",
	},
	{
		name: "stop-fee",
		title: "Stop Fee %:",
		defaultValue: 0.01,
		type: "number",
		min: 0,
		max: 100,
		lockable: true,
	},
	{
		name: "take-profit",
		title: "Take Profit:",
		type: "number",
	},
	{
		name: "take-profit-fee",
		title: "Take Profit Fee %:",
		defaultValue: 0.01,
		type: "number",
		min: 0,
		max: 100,
		lockable: true,
	},
];

const states = fields.reduce<{
	[key in string]: {
		field: (typeof fields)[number];
		useValue: () => any;
		// valueChange$: Observable<any>;
		setValue: (payload: any) => void;
		value$: Observable<string | number | undefined>;
		lock$?: Observable<boolean>;
		useLock?: () => boolean;
		toggleLock?: () => void;
	};
}>((acc, field) => {
	const [valueChange$, setValue] = createSignal<string | number | undefined>();
	const initValue = localStorage.getItem(field.name);

	const [useValue, value$] = bind(
		valueChange$,
		initValue || field.defaultValue
	);

	value$
		.pipe(debounceTime(250))
		.subscribe((v) => localStorage.setItem(field.name, v as string));

	acc[field.name] = {
		field: field,
		useValue: useValue,
		setValue: setValue,
		value$: value$.pipe(
			debounceTime(200),
			map((v) => {
				if (v == undefined) {
					return v;
				}

				if (field.type == "number") {
					return Number(v);
				}

				return String(v);
			})
		),
	};

	if (field.lockable) {
		const lockKey = `${field.name}.lock`;
		const [lockChange$, setLock] = createSignal<boolean>();
		const lockInitValue = localStorage.getItem(lockKey);

		const [useLock, lockValue$] = bind(lockChange$, Boolean(lockInitValue));

		acc[field.name].lock$ = lockValue$;
		acc[field.name].toggleLock = () => setLock(!lockValue$.getValue());
		acc[field.name].useLock = useLock;

		lockValue$
			.pipe(debounceTime(250))
			.subscribe((v) => localStorage.setItem(lockKey, String(v)));
	}

	return acc;
}, {});

// merge(
// 	...Object.entries(states).map(([key, methods]) => {
// 		return methods.value$.pipe(map((v) => [key, v]));
// 	})
// ).subscribe((v) => {
// 	console.log(v);
// });

const [useRiskInUSD, riskInUSD$] = bind(
	scheduled(
		[states["account-balance"].value$, states["risk-percent"].value$],
		asyncScheduler
	).pipe(
		combineLatestAll(),
		map((values: any) => {
			const [account_balance, risk_percent] = values as number[];
			return (account_balance * risk_percent) / 100;
		})
	),
	0
);

const [useTrend, _trend$] = bind(
	scheduled(
		[
			states["entry-price"].value$,
			states["stoploss"].value$,
			states["take-profit"].value$,
		],
		asyncScheduler
	).pipe(
		combineLatestAll(),
		map((values) => {
			const [entryPrice, stoploss, takeProfit] = values as number[];
			if (!entryPrice || !stoploss) {
				return undefined;
			}

			if (
				(entryPrice > stoploss && entryPrice > takeProfit) ||
				(entryPrice < stoploss && entryPrice < takeProfit)
			) {
				return "Unknown";
			}
			return entryPrice > stoploss ? "Long" : "Short";
		})
	),
	undefined
);

const [usePositionSizeUSD, positionSizeUSD$] = bind(
	scheduled(
		[
			states["entry-price"].value$,
			riskInUSD$,
			states["stoploss"].value$,
			states["stop-fee"].value$,
			states["entry-fee"].value$,
		],
		asyncScheduler
	).pipe(
		combineLatestAll(),
		map((values) => {
			const [entryPrice, riskInUSD, stoploss, stopFee, entryFee] =
				values as number[];

			const lossRemainedPercent =
				entryPrice < stoploss ? entryPrice / stoploss : stoploss / entryPrice;
			return (
				riskInUSD /
				(entryFee / 100 +
					(stopFee / 100) * lossRemainedPercent +
					(1 - lossRemainedPercent))
			);
		})
	),
	undefined
);

const [usePositionSizeCrypto, _positionSizeCrypto$] = bind(
	scheduled(
		[states["entry-price"].value$, positionSizeUSD$],
		asyncScheduler
	).pipe(
		combineLatestAll(),
		map((values: any) => {
			const [entryPrice, positionSizeUSD] = values as number[];

			return positionSizeUSD / entryPrice;
		})
	),
	undefined
);
const [useFeeInUSD, _feeInUSD$] = bind(
	scheduled(
		[
			states["entry-price"].value$,
			states["stoploss"].value$,
			states["stop-fee"].value$,
			states["entry-fee"].value$,
			positionSizeUSD$,
		],
		asyncScheduler
	).pipe(
		combineLatestAll(),
		map((values: any) => {
			const [entryPrice, stoploss, stopFee, entryFee, positionSizeUSD] =
				values as number[];

			return (
				positionSizeUSD *
				(entryFee / 100 + (stopFee / 100) * (entryPrice / stoploss))
			);
		})
	),
	undefined
);

const [useEntryOverStoplossRatio, entryOverStoplossRatio$] = bind(
	scheduled(
		[states["entry-price"].value$, states["stoploss"].value$, positionSizeUSD$],
		asyncScheduler
	).pipe(
		combineLatestAll(),
		map((values: any) => {
			const [entryPrice, stoploss] = values as number[];

			return entryPrice > stoploss
				? 1 - stoploss / entryPrice
				: 1 - entryPrice / stoploss;
		})
	),
	undefined
);

const [useTradeProfit, tradeProfit$] = bind(
	scheduled(
		[
			states["entry-price"].value$,
			states["entry-fee"].value$,
			states["take-profit"].value$,
			states["take-profit-fee"].value$,
			positionSizeUSD$,
		],
		asyncScheduler
	).pipe(
		combineLatestAll(),
		map((values: any) => {
			const [entryPrice, entryFee, takeProfit, takeProfitFee, positionSize] =
				values as number[];

			const TPRatio =
				entryPrice > takeProfit
					? entryPrice / takeProfit
					: takeProfit / entryPrice;

			return (
				positionSize *
				Math.abs(
					1 - (TPRatio - entryFee / 100 + TPRatio * (takeProfitFee / 100))
				)
			);
		})
	),
	undefined
);

const [useTradeProfitRatio, _tradeProfitRatio$] = bind(
	scheduled([positionSizeUSD$, tradeProfit$], asyncScheduler).pipe(
		combineLatestAll(),
		map((values: any) => {
			const [positionSize, tradeProfit] = values as number[];

			return tradeProfit / positionSize;
		})
	),
	undefined
);

const [useRealProfitPercent, _realProfitPercent$] = bind(
	scheduled(
		[states["account-balance"].value$, tradeProfit$],
		asyncScheduler
	).pipe(
		combineLatestAll(),
		map((values: any) => {
			const [balance, tradeProfit] = values as number[];

			return tradeProfit / balance;
		})
	),
	undefined
);

const [useEntryOverTakeProfitRatio, entryOverTakeProfitRatio$] = bind(
	scheduled(
		[states["entry-price"].value$, states["take-profit"].value$],
		asyncScheduler
	).pipe(
		combineLatestAll(),
		map((values: any) => {
			const [entry, takeProfit] = values as number[];

			return 1 - (takeProfit < entry ? takeProfit / entry : entry / takeProfit);
		})
	),
	undefined
);

const [useRiskRewardRatio, _riskRewardRatio$] = bind(
	scheduled(
		[entryOverTakeProfitRatio$, entryOverStoplossRatio$],
		asyncScheduler
	).pipe(
		combineLatestAll(),
		map((values: any) => {
			const [entryOverTakeProfitRatio, entryOverStoplossRatio] =
				values as number[];
			return entryOverTakeProfitRatio / entryOverStoplossRatio;
		})
	),
	undefined
);

function rounding(num: number | string) {
	if (typeof num == "string") {
		return num;
	}
	const l = Math.floor(Math.log10(num));

	if (l > 0) {
		return num.toString().slice(0, l + 4);
	}
	return num.toFixed(-l + 3);
}

function ComputePositionSize() {
	const _states = Object.entries(states).reduce((acc, [fieldName, methods]) => {
		acc[fieldName] = methods.useValue();
		return acc;
	}, {} as { [key in string]: any });

	const _lockStates = Object.entries(states).reduce(
		(acc, [fieldName, methods]) => {
			if (methods.useLock) {
				acc[fieldName] = methods.useLock();
			}
			return acc;
		},
		{} as { [key in string]: boolean }
	);

	const riskInUSD = useRiskInUSD();
	const trend = useTrend();
	const positionSizeUSD = usePositionSizeUSD();
	const positionSizeCrypto = usePositionSizeCrypto();
	const feeInUSD = useFeeInUSD();
	const entryOverStoplossRatio = useEntryOverStoplossRatio();
	const tradeProfit = useTradeProfit();
	const tradeProfitRatio = useTradeProfitRatio();
	const realProfit = useRealProfitPercent();

	const entryOverTakeProfitRatio = useEntryOverTakeProfitRatio();
	const riskRewardRatio = useRiskRewardRatio();
	const reset = useCallback(
		() =>
			Object.entries(states).forEach(([key, methods]) => {
				if (!_lockStates[key]) {
					methods.setValue(methods.field.defaultValue || "");
				}
			}),
		[_lockStates]
	);
	const copy = useCallback((value: string) => {
		navigator.clipboard.writeText(value);
		toast.success(`Copied ${value || ""}`, {});
	}, []);

	return (
		<Subscribe>
			<div className="flex flex-col w-full gap-4 md:w-4/5 lg:w-3/5  mx-auto">
				<h5
					className={classNames("mx-auto text-md text-[#1d82f6] font-bold", {
						"text-green-500": trend == "Long",
						"text-red-700": trend == "Short",
					})}
				>
					{trend && `You are open ${trend} position !`} &thinsp;
				</h5>

				<div className="flex gap-4 flex-col">
					{fields.map((f) => {
						return (
							<div
								key={`${f.name}-field`}
								className="flex gap-2 bg-[#2e2f30] rounded-md"
							>
								<label
									htmlFor={f.name}
									className="text-[#a1a5ab] basis-44 py-3 pl-4 text-sm my-auto font-semibold"
								>
									{f.title}
								</label>
								<div className="bg-[#272829] w-full text-sm font-medium flex-1 rounded-r-md flex">
									<input
										name={f.name}
										type={f.type}
										disabled={_lockStates[f.name]}
										value={_states[f.name] as any}
										onChange={(e) => states[f.name].setValue(e.target.value)}
										className="w-full outline-none py-3 px-2 bg-inherit disabled:text-[#1d82f6] overflow-auto"
										min={f.min}
										max={f.max}
										step={f.step}
									></input>
									<div className="flex gap-2 text-base">
										{f.lockable && (
											<button
												className="opacity-25 hover:opacity-100 transition delay-75"
												onClick={() => {
													if (states[f.name].toggleLock) {
														states[f.name].toggleLock!();
													}
												}}
											>
												{_lockStates[f.name] ? "🔒" : "🔓"}
											</button>
										)}
										<button
											className="opacity-25 hover:opacity-10 transition delay-75"
											onClick={() => copy(_states[f.name])}
										>
											📋
										</button>
									</div>
								</div>
							</div>
						);
					})}
				</div>
				<div className="flex justify-center gap-4">
					<button
						className="hover:text-[#1d82f6] opacity-25 hover:opacity-100 transition delay-75 "
						onClick={() => reset()}
					>
						Reset all
					</button>
				</div>
				<table className="min-w-full divide-y divide-gray-200 dark:divide-neutral-700">
					<thead>
						<tr>
							{[
								"Risk In USD",
								"Position Size USD",
								"Position Size Crypto",
								"Fee In USD",
								"Relative Stoploss/Entry %",
							].map((header) => (
								<th
									scope="col"
									className="px-6 py-3 text-start text-xs font-medium text-[#a1a5ab] uppercase "
								>
									{header}
								</th>
							))}
						</tr>
					</thead>
					<tbody>
						<tr>
							{[
								riskInUSD,
								positionSizeUSD,
								positionSizeCrypto,
								feeInUSD,
								(entryOverStoplossRatio || 0) * 100,
							].map((value) => (
								<td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-[#1d82f6]">
									{value ? rounding(value) : "NaN"}
								</td>
							))}
						</tr>
					</tbody>
					<thead>
						<tr>
							{[
								"Trade Profit in USD",
								"Trade Profit %",
								"Real Profit %",
								"Relative TakeProfit/Entry %",
								"Risk-Reward ratio",
							].map((header) => (
								<th
									scope="col"
									className="px-6 py-3 text-start text-xs font-medium text-[#a1a5ab] uppercase "
								>
									{header}
								</th>
							))}
						</tr>
					</thead>
					<tbody>
						<tr>
							{[
								tradeProfit,
								(tradeProfitRatio || 0) * 100,
								(realProfit || 0) * 100,
								(entryOverTakeProfitRatio || 0) * 100,
								riskRewardRatio,
							].map((value) => (
								<td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-[#1d82f6]">
									{value ? rounding(value) : "NaN"}
								</td>
							))}
						</tr>
					</tbody>
				</table>
			</div>
		</Subscribe>
	);
}

function App() {
	return (
		<>
			<main className="container px-4 mx-auto text-white gap-16 flex flex-col mt-8 mb-8">
				<section className="items-center flex flex-col mx-auto gap-4 mt-8">
					<h1 className="text-4xl lg:text-5xl font-bold">
						Crypto Position Size Calculator!
					</h1>
					<p className="text-md mx-auto">
						Calculate your crypto position size according to account balance,
						risk, entry price, stop loss and exchange trading fees.
					</p>
				</section>
				<section className="flex flex-row">
					<ComputePositionSize />
				</section>
				<footer className="bottom-0 w-full flex justify-center py-3 border-t-[1px] border-white bg-[#1f2122]">
					<p className="animate-pulse">
						Ho Trung Nhan ©{new Date().getFullYear()}
					</p>
				</footer>
			</main>
			<ToastContainer
				autoClose={200}
				hideProgressBar
				stacked
				position="bottom-center"
				draggable={false}
				closeButton={false}
				theme="dark"
				className={"mb-4 md:mb-4"}
				toastClassName={
					"text-center rounded-md w-fit mx-auto px-2 py-1 text-white min-h-fit"
				}
			/>
		</>
	);
}

export default App;
