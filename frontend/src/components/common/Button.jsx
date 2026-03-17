import React from 'react';
import clsx from 'clsx';
import { Loader2 } from 'lucide-react';

const VARIANTS = {
  // Primary: Strong Emerald Glow
  primary: "bg-emerald-600 hover:bg-emerald-500 text-white shadow-lg shadow-emerald-500/20 border border-transparent ring-emerald-500",

  // Secondary: Glass Effect (See-through slate)
  secondary: "bg-slate-800/50 backdrop-blur-sm hover:bg-slate-700/50 text-slate-200 border border-white/10 hover:border-white/20 ring-slate-500",

  // Danger: Red Glow
  danger: "bg-rose-600 hover:bg-rose-500 text-white shadow-lg shadow-rose-500/20 ring-rose-500",

  // Ghost: Minimal hover effect
  ghost: "bg-transparent hover:bg-white/5 text-slate-400 hover:text-white ring-slate-500",
};

const SIZES = {
  xs: "px-2 py-1 text-[10px]",
  sm: "px-3 py-1.5 text-xs",
  md: "px-4 py-2 text-sm",
  lg: "px-6 py-3 text-base",
  icon: "p-2 aspect-square" // Ensures round/square icon buttons
};

export default function Button({
  children,
  variant = 'primary',
  size = 'md',
  className,
  isLoading,
  icon: Icon,
  disabled,
  ...props
}) {
  return (
    <button
      className={clsx(
        // Base Layout & Animation
        "inline-flex items-center justify-center font-semibold rounded-lg transition-all duration-200",
        // Interaction
        "active:scale-95 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-slate-950",
        // Disabled State
        "disabled:opacity-50 disabled:pointer-events-none disabled:cursor-not-allowed",
        VARIANTS[variant],
        SIZES[size],
        className
      )}
      disabled={isLoading || disabled}
      {...props}
    >
      {isLoading ? (
        <Loader2 className={clsx("animate-spin", size === 'icon' ? "w-5 h-5" : "w-4 h-4 mr-2")} />
      ) : Icon ? (
        <Icon className={clsx("opacity-90", size === 'icon' ? "w-5 h-5" : "w-4 h-4 mr-2")} />
      ) : null}

      {/* Hide text if it's an icon-only button and loading, otherwise show it */}
      {!(size === 'icon' && isLoading) && children}
    </button>
  );
}
