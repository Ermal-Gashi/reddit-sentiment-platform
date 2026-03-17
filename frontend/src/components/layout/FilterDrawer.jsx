import React from 'react';
import { X, Calendar, Filter } from 'lucide-react';
import clsx from 'clsx';
import Button from '../common/Button';

export default function FilterDrawer({ isOpen, onClose }) {
  return (
    <>
      {/* Backdrop */}
      <div
        className={clsx(
          "fixed inset-0 bg-black/60 backdrop-blur-sm z-40 transition-opacity duration-300",
          isOpen ? "opacity-100" : "opacity-0 pointer-events-none"
        )}
        onClick={onClose}
      />

      {/* Drawer Panel (Now on the LEFT side) */}
      <aside
        className={clsx(
          "fixed top-0 left-0 h-full w-80 bg-slate-900 border-r border-slate-800 shadow-2xl z-50 transform transition-transform duration-300 ease-out flex flex-col",
          isOpen ? "translate-x-0" : "-translate-x-full"
        )}
      >
        <div className="flex items-center justify-between p-6 border-b border-slate-800 h-20">
          <h2 className="text-lg font-bold text-white flex items-center gap-2">
            <Filter className="text-emerald-500 w-5 h-5" />
            Filters
          </h2>
          <Button variant="ghost" size="icon" onClick={onClose}>
            <X className="w-5 h-5" />
          </Button>
        </div>

        <div className="flex-1 p-6 space-y-8 overflow-y-auto">
          {/* Example Filter: Date */}
          <div className="space-y-3">
            <label className="text-xs font-bold text-slate-500 uppercase tracking-widest">Time Range</label>
            <div className="flex items-center gap-3 p-3 bg-slate-950 border border-slate-800 rounded-lg text-slate-300 cursor-pointer hover:border-emerald-500/50 transition-colors">
              <Calendar className="w-4 h-4 text-slate-500" />
              <span className="text-sm font-mono">Nov 20, 2025</span>
            </div>
          </div>
        </div>

        <div className="p-6 border-t border-slate-800">
          <Button variant="primary" className="w-full" onClick={onClose}>
            Apply Filters
          </Button>
        </div>
      </aside>
    </>
  );
}