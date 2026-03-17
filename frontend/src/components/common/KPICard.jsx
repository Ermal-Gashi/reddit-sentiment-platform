import React from 'react';
import { ArrowUpRight, ArrowDownRight, Minus } from 'lucide-react';
import clsx from 'clsx';

const THEME = {
  base: "border rounded-2xl overflow-hidden transition-all duration-300 ease-out group",
  hover: "hover:border-emerald-500/50 hover:shadow-[0_0_30px_-5px_rgba(249,115,22,0.15)] hover:-translate-y-1",
};

export default function KPICard({
  title,
  value,
  trend = 0,
  trendLabel,
  icon: Icon,
  screenshot = false, //  NEW
}) {
  const isPositive = trend > 0;
  const isNeutral = trend === 0;
  const isNegative = trend < 0;

  return (
    <div
      className={clsx(
        "relative p-6",
        THEME.base,
        !screenshot && "bg-navy-700 border-white/5",
        screenshot && "bg-white border-slate-200",
        !screenshot && THEME.hover
      )}
    >

      {/* BACKGROUND GLOW (disabled in screenshot mode) */}
      {!screenshot && (
        <div className="absolute inset-0 bg-gradient-to-br from-emerald-500/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-500" />
      )}

      {/* WATERMARK ICON */}
      {Icon && (
        <div
          className={clsx(
            "absolute -right-6 -top-6 rotate-12 transition-colors duration-500",
            screenshot ? "text-slate-200" : "text-white/5 group-hover:text-emerald-500/10"
          )}
        >
          <Icon size={120} strokeWidth={1} />
        </div>
      )}

      <div className="relative z-10 flex flex-col h-full justify-between space-y-4">

        {/* HEADER */}
        <div className="flex items-center justify-between">
          <span
            className={clsx(
              "text-xs font-bold uppercase tracking-widest",
              screenshot ? "text-slate-600" : "text-slate-400"
            )}
          >
            {title}
          </span>

          {Icon && (
            <Icon
              size={20}
              className={clsx(
                "transition-colors duration-300",
                screenshot ? "text-slate-600" : "text-slate-600 group-hover:text-emerald-500"
              )}
            />
          )}
        </div>

        {/* VALUE */}
        <div>
          <span
            className={clsx(
              "text-5xl font-black tracking-tighter font-mono",
              screenshot ? "text-slate-900" : "text-white drop-shadow-lg"
            )}
          >
            {value}
          </span>
        </div>

        {/* TREND */}
        <div className="flex items-center gap-3">
          <div
            className={clsx(
              "flex items-center gap-1.5 px-2.5 py-1 rounded-md text-sm font-bold border",
              isPositive
                ? screenshot
                  ? "bg-orange-100 text-orange-700 border-orange-200"
                  : "bg-emerald-500/10 text-emerald-500 border-emerald-500/20 group-hover:bg-emerald-500/20"
                : screenshot
                ? "bg-slate-100 text-slate-600 border-slate-200"
                : "bg-white/5 text-slate-400 border-white/5 group-hover:bg-white/10"
            )}
          >
            {isPositive && <ArrowUpRight size={14} strokeWidth={3} />}
            {isNeutral && <Minus size={14} strokeWidth={3} />}
            {isNegative && <ArrowDownRight size={14} strokeWidth={3} />}
            {Math.abs(trend)}%
          </div>

          <span
            className={clsx(
              "text-[10px] font-medium uppercase tracking-wide",
              screenshot ? "text-slate-500" : "text-slate-500"
            )}
          >
            {trendLabel}
          </span>
        </div>
      </div>

      {/* BOTTOM ACCENT (disabled in screenshot mode) */}
      {!screenshot && (
        <div className="absolute bottom-0 left-0 w-full h-[2px] bg-gradient-to-r from-emerald-500 to-transparent scale-x-0 group-hover:scale-x-100 transition-transform duration-500 origin-left" />
      )}
    </div>
  );
}
