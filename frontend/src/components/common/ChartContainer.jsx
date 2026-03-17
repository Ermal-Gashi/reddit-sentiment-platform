import React, { useState } from 'react';
import { Maximize2, Minimize2, Info, X } from 'lucide-react';
import clsx from 'clsx';
import Button from './Button';

export default function ChartContainer({
  title,
  subtitle,
  children,
  controls,
  infoText = "No detailed methodology available.",
  height = "h-80",
  reportMode = false //  NEW: light mode for thesis screenshots
}) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [showInfo, setShowInfo] = useState(false);
  const toggleExpand = () => setIsExpanded(!isExpanded);

  const actionBtnClass = clsx(
    "h-8 w-8 rounded-lg flex items-center justify-center transition-all duration-200",
    reportMode
      ? "text-slate-600 hover:text-emerald-500 bg-white hover:bg-slate-100 border border-slate-200"
      : "text-slate-500 hover:text-emerald-500 bg-transparent hover:bg-navy-900 border border-transparent hover:border-white/10"
  );

  return (
    <>
      <div
        className={clsx(
          "cursor-emerald flex flex-col overflow-hidden transition-all duration-300 ease-[cubic-bezier(0.25,0.1,0.25,1.0)]",
          reportMode
            ? "bg-white border border-slate-200 text-slate-900"
            : "bg-navy-700 border border-white/5 text-white",

          isExpanded
            ? reportMode
              ? "fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[90vw] h-[80vh] z-[100] rounded-2xl shadow-xl"
              : "fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[90vw] h-[80vh] z-[100] rounded-2xl shadow-2xl border-emerald-500/30 ring-4 ring-black/50"
            : reportMode
              ? "relative rounded-xl shadow-md"
              : "relative rounded-xl shadow-lg shadow-black/20 hover:shadow-emerald-500/5 hover:border-emerald-500/30"
        )}
      >

        {/* HEADER */}
        <div className="flex-none flex flex-col pt-4 px-5 pb-3">
          <div className="flex items-center justify-between mb-3">
            <div>
              {title && (
                <h3
                  className={clsx(
                    "text-xs font-extrabold uppercase tracking-wider",
                    reportMode ? "text-slate-900" : "text-white"
                  )}
                >
                  {title}
                </h3>
              )}

              {subtitle && (
                <p
                  className={clsx(
                    "text-[10px] font-medium mt-0.5",
                    reportMode ? "text-slate-600" : "text-slate-400"
                  )}
                >
                  {subtitle}
                </p>
              )}
            </div>

            <div className="flex items-center gap-3">
              {controls && (
                <div className={clsx(
                  "flex items-center mr-2 pr-4",
                  reportMode ? "border-r border-slate-200" : "border-r border-white/5"
                )}>
                  {controls}
                </div>
              )}

              <div className="flex items-center gap-1">
                <button onClick={() => setShowInfo(true)} className={actionBtnClass}>
                  <Info size={16} />
                </button>
                <button onClick={toggleExpand} className={actionBtnClass}>
                  {isExpanded ? <Minimize2 size={16} /> : <Maximize2 size={16} />}
                </button>
              </div>
            </div>
          </div>

          <div
            className={clsx(
              "h-[1px] w-full",
              reportMode
                ? "bg-slate-200"
                : "bg-gradient-to-r from-emerald-500/20 to-transparent"
            )}
          />
        </div>

        {/* BODY */}
        <div
          className={clsx(
            "relative w-full px-4 pb-4 overflow-hidden",
            reportMode ? "bg-white" : "bg-navy-700",
            isExpanded ? "flex-1 h-full" : height
          )}
        >
          {children}
        </div>
      </div>

      {/* MODAL OVERLAY */}
      {isExpanded && (
        <div
          className={clsx(
            "fixed inset-0 z-[90]",
            reportMode ? "bg-white/70" : "bg-black/80 backdrop-blur-sm"
          )}
          onClick={toggleExpand}
        />
      )}

      {/* INFO MODAL */}
      {showInfo && (
        <div className="fixed inset-0 z-[110] flex items-center justify-center p-4">
          <div
            className={clsx(
              "absolute inset-0",
              reportMode ? "bg-white/70" : "bg-black/60 backdrop-blur-sm"
            )}
            onClick={() => setShowInfo(false)}
          />
          <div
            className={clsx(
              "relative rounded-xl shadow-2xl w-full max-w-md overflow-hidden",
              reportMode
                ? "bg-white border border-slate-200"
                : "bg-navy-800 border border-white/10"
            )}
          >
            <div className={clsx(
              "p-5 flex justify-between items-center",
              reportMode ? "border-b border-slate-200" : "border-b border-white/5"
            )}>
              <h3 className={clsx(
                "text-sm font-bold uppercase",
                reportMode ? "text-slate-900" : "text-white"
              )}>
                Data Methodology
              </h3>
              <button onClick={() => setShowInfo(false)}>
                <X size={18} className={reportMode ? "text-slate-500" : "text-slate-500 hover:text-white"} />
              </button>
            </div>

            <div className={clsx(
              "p-6 text-xs leading-relaxed whitespace-pre-line font-mono",
              reportMode ? "text-slate-700" : "text-slate-300 opacity-80"
            )}>
              {infoText}
            </div>
          </div>
        </div>
      )}
    </>
  );
}
