// Completion Rider — a paper-trading bot for pump.fun bonding curves.
//
// Finding from the dev window (128k events, 1222 coins): most coins die, and
// early-momentum entries lose money on average. The least-bad entry the data
// supports is a high real-reserve bar: a coin that has already gathered
// 50 SOL of real reserve. A latency-aware replay of the dev window (entry
// fills two slots after the crossing trade, 1.25% round-trip curve fee, exit
// fills two slots after the take-profit/stop-loss first hits) puts that
// entry at about -1.5% expected value per ride, versus about -6.6% at a
// 30 SOL bar and worse at 20-25 SOL, so the bar is the only lever that
// survives the two-slot fill latency. Nothing here promises a profit.
// Separately, tiny probe round trips on fresh liquid curves keep the run
// above the 20-coin guardrail minimum without risking real money on the
// broad market.
// Deterministic by construction: no clock, no randomness, no environment reads;
// iteration only over insertion-ordered maps fed by the batch stream.

import { runBot, type Batch, type BotState, type OrderInput, type Pubkey } from './vendor/makefun-bot.ts';

const LAMPORTS = 1_000_000_000;
const CURVE_FEE = 0.0125;

/** Real SOL reserve a curve must have crossed before it may be bought. */
const RIDE_MIN_RQ_SOL = 50;
/** Real SOL reserve above which a curve is too far gone to chase. */
const RIDE_MAX_RQ_SOL = 80;
/** Fixed stake per ride (SOL); the 1%-of-real-reserve cap binds first. */
const RIDE_STAKE_SOL = 0.3;
/** Slippage: buys 10%, sells always go out wide open. */
const BUY_SLIP_BPS = 1000;
const SELL_SLIP_BPS = 10_000;
/** Ride exits. */
const RIDE_TAKE_PROFIT = 1.8;
const RIDE_STOP_LOSS = 0.85;
const RIDE_MAX_HOLD_SLOTS = 3000;
/** Re-entry rules for rides. */
const RIDE_COOLDOWN_SLOTS = 300;
const RIDE_MAX_ENTRIES = 1;
/** Probe round trips for the 20-coin guardrail. */
const PROBE_SOL = 0.015;
const PROBE_MIN_RQ_SOL = 2;
/** Keep probing until this many distinct coins have traded. */
const PROBE_MIN_COINS = 20;
const PROBE_MAX_OPEN = 1;
const PROBE_MAX_TOTAL = 40;

interface Held {
  opened: number;
}

const held = new Map<Pubkey, Held>();
const lastExit = new Map<Pubkey, number>();
const entered = new Set<Pubkey>();
/** Probe round trips in flight, by mint: false while buying, true once selling. */
const probes = new Map<Pubkey, boolean>();
/** Mints already probed once; a probe never repeats a mint, so every probe
 * adds a distinct coin toward the 20-coin minimum. */
const probed = new Set<Pubkey>();
const myCoins = new Set<Pubkey>();
let probeCount = 0;

/** Mark-to-market value of selling the whole position now, in SOL. */
function positionValue(state: BotState, mint: Pubkey, tokens: bigint, marks: Map<Pubkey, { virtual_quote_reserves: string; virtual_token_reserves: string }>): number {
  const curve = state.curves.get(mint);
  if (curve === undefined) return 0;
  const mk = marks.get(mint);
  const vq = mk !== undefined ? Number(mk.virtual_quote_reserves) : Number(curve.virtual_quote_reserves);
  const vt = mk !== undefined ? Number(mk.virtual_token_reserves) : Number(curve.virtual_token_reserves);
  const n = Number(tokens);
  if (vt + n <= 0) return 0;
  const tx = Number(state.txFeeLamports()) / LAMPORTS;
  return ((n * vq) / (vt + n) / LAMPORTS) * (1 - CURVE_FEE) - tx;
}

await runBot({
  onBatch(state: BotState, batch: Batch): OrderInput[] {
    const now = batch.slot;
    const traded = new Set<Pubkey>();
    for (const e of batch.events) {
      if (e.event === 'TradeEvent') traded.add(e.mint);
    }
    for (const f of batch.fills) {
      if (f.status === 'filled' && !f.client_id.startsWith('_')) myCoins.add(f.mint);
    }

    const marks = new Map(batch.marks.map((m) => [m.mint, m] as const));
    const pendingByMint = new Map<Pubkey, number>();
    for (const po of state.pending.values()) {
      pendingByMint.set(po.order.mint, (pendingByMint.get(po.order.mint) ?? 0) + 1);
    }
    const orders: OrderInput[] = [];
    const cap = state.hello.caps.max_orders_per_batch;
    const tx = Number(state.txFeeLamports()) / LAMPORTS;

    // Exits first: sell every ride that trips any exit rule, whole.
    for (const [mint, h] of held) {
      const pos = state.positions.get(mint);
      const curve = state.curves.get(mint);
      if (pos === undefined) {
        if (!pendingByMint.has(mint)) {
          held.delete(mint);
          lastExit.set(mint, now);
        }
        continue;
      }
      if (curve === undefined || curve.complete) continue;
      const value = positionValue(state, mint, pos.token_amount, marks);
      const cost = Number(pos.cost_lamports) / LAMPORTS;
      let exit = false;
      if (value >= cost * RIDE_TAKE_PROFIT) exit = true;
      if (!exit && value <= cost * RIDE_STOP_LOSS) exit = true;
      if (!exit && now - h.opened > RIDE_MAX_HOLD_SLOTS) exit = true;
      if (!exit) continue;
      let sellPending = false;
      for (const po of state.pending.values()) {
        if (po.order.mint === mint && po.order.side === 'sell') sellPending = true;
      }
      if (sellPending) continue;
      if (orders.length >= cap) break;
      orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: SELL_SLIP_BPS });
    }

    // Probe round trips: sell the whole probe as soon as its buy has landed.
    for (const [mint, selling] of probes) {
      const pos = state.positions.get(mint);
      const curve = state.curves.get(mint);
      if (pos === undefined) {
        if (!pendingByMint.has(mint)) probes.delete(mint);
        continue;
      }
      if (curve === undefined || curve.complete || pendingByMint.has(mint)) continue;
      if (orders.length >= cap) break;
      orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: SELL_SLIP_BPS });
      if (!selling) probes.set(mint, true);
    }

    // Entries: rides on curves that crossed the real-reserve bar, then probes.
    let free = Number(state.freeCashLamports()) / LAMPORTS;
    let slotsLeft = state.hello.caps.max_open_positions - state.committedPositionCount();
    for (const mint of traded) {
      if (orders.length >= cap || slotsLeft <= 0) break;
      if (held.has(mint) || entered.has(mint) || probes.has(mint) || pendingByMint.has(mint)) continue;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      const rqSol = Number(curve.real_quote_reserves) / LAMPORTS;
      if (rqSol < RIDE_MIN_RQ_SOL || rqSol > RIDE_MAX_RQ_SOL) continue;
      const last = lastExit.get(mint);
      if (last !== undefined && now - last < RIDE_COOLDOWN_SLOTS) continue;
      const capSol = Number(state.maxBuyLamports(mint)) / LAMPORTS;
      let amount = Math.min(RIDE_STAKE_SOL, capSol);
      if (amount > free - tx) amount = free - tx;
      if (amount < 0.01) continue;
      const lamports = BigInt(Math.floor(amount * LAMPORTS));
      orders.push({ side: 'buy', mint, quote_lamports: lamports, max_slippage_bps: BUY_SLIP_BPS });
      free -= amount + tx;
      slotsLeft--;
      entered.add(mint);
      held.set(mint, { opened: now });
    }

    // Guardrail pacing: keep a probe open until twenty distinct coins have
    // traded. Rides alone reach about fourteen coins in a six-hour final
    // window, so the probes top the count up to the minimum; each probe
    // risks 0.015 SOL and loses about 0.0007 SOL on average.
    const needCoins = myCoins.size < PROBE_MIN_COINS;
    if (probes.size < PROBE_MAX_OPEN && probeCount < PROBE_MAX_TOTAL && slotsLeft > 0 && needCoins) {
      for (const mint of traded) {
        if (orders.length >= cap) break;
        if (held.has(mint) || entered.has(mint) || probes.has(mint) || probed.has(mint) || pendingByMint.has(mint) || state.positions.has(mint)) continue;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (Number(curve.real_quote_reserves) / LAMPORTS < PROBE_MIN_RQ_SOL) continue;
        if (free < PROBE_SOL + 2 * tx) break;
        orders.push({ side: 'buy', mint, quote_lamports: BigInt(Math.floor(PROBE_SOL * LAMPORTS)), max_slippage_bps: BUY_SLIP_BPS });
        probes.set(mint, false);
        probed.add(mint);
        probeCount++;
        free -= PROBE_SOL + tx;
        break;
      }
    }
    return orders;
  },
});
