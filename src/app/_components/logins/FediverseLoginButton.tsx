'use client'

import { useState } from 'react'
import { motion } from 'framer-motion'
import { Globe2, ArrowRight, CheckCircle2, Loader2, AlertCircle } from 'lucide-react'
import { quantico } from '@/app/fonts/plex'
import { useTheme } from '@/hooks/useTheme'

interface FediverseLoginButtonProps {
  onLoadingChange?: (loading: boolean) => void
  onError?: (error: string) => void
  isConnected?: boolean
  isSelected?: boolean
  className?: string
  onClick?: () => void
  showForm?: boolean
  userId?: string
}

export default function FediverseLoginButton({
  onLoadingChange = () => {},
  onError = () => {},
  isConnected = false,
  isSelected = false,
  className = '',
  onClick = () => {},
  showForm = false,
  userId,
}: FediverseLoginButtonProps) {
  const { isDark } = useTheme()
  const [handleText, setHandleText] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(false)

  const inputClasses = isDark
    ? 'bg-white/5 border-white/20 text-white placeholder-white/40 focus:border-emerald-400'
    : 'bg-slate-50 border-slate-200 text-slate-900 placeholder-slate-400 focus:border-emerald-500'

  const validateHandle = (value: string): boolean => {
    const trimmed = value.trim()
    if (!trimmed) {
      setError('Handle or instance is required')
      return false
    }
    if (/\s/.test(trimmed)) {
      setError('No spaces allowed')
      return false
    }
    const host = trimmed.startsWith('@') ? trimmed.split('@').filter(Boolean).pop() : trimmed
    if (!host || !host.includes('.')) {
      setError('Please include a valid instance domain')
      return false
    }
    setError(null)
    return true
  }

  const handleSubmit = async () => {
    if (!validateHandle(handleText)) return

    try {
      setIsLoading(true)
      onLoadingChange(true)

      const response = await fetch('/api/auth/fediverse/init', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          handle: handleText.trim(),
          redirect: false,
          redirectTo: '/reconnect',
          userId,
        }),
      })

      const payload = await response.json().catch(() => ({}))

      if (!response.ok) {
        const message = typeof payload?.error === 'string' ? payload.error : 'Unable to start Fediverse OAuth'
        setError(message)
        onError(message)
        return
      }

      if (typeof payload?.authorizationUrl === 'string') {
        window.location.href = payload.authorizationUrl
        return
      }

      const message = 'Missing authorization URL'
      setError(message)
      onError(message)
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Unknown error'
      setError(message)
      onError(message)
    } finally {
      setIsLoading(false)
      onLoadingChange(false)
    }
  }

  if (!showForm) {
    return (
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        exit={{ opacity: 0, y: -12 }}
        className="w-full"
      >
        <motion.button
          whileHover={{ scale: 1.01 }}
          whileTap={{ scale: 0.99 }}
          onClick={onClick}
          disabled={isConnected}
          className={`${quantico.className} ${className} group relative w-full rounded-2xl border border-emerald-500/30 bg-gradient-to-r from-emerald-500 via-teal-500 to-cyan-500 p-5 text-left transition-all duration-300 shadow-[0_0_25px_rgba(16,185,129,0.25)] hover:shadow-[0_0_35px_rgba(16,185,129,0.35)] hover:border-emerald-400/50 disabled:opacity-70 disabled:cursor-not-allowed`}
        >
          <div className="relative flex items-center gap-4">
            <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-white/20 backdrop-blur-sm">
              <Globe2 className="h-6 w-6 text-white" />
            </div>
            <div className="flex-1">
              <p className="text-base font-semibold text-white">
                {isConnected ? 'Connected' : 'Fediverse'}
              </p>
              <p className="text-xs text-white/80">Connect any ActivityPub instance</p>
            </div>
            <div className="flex h-10 w-10 items-center justify-center rounded-full bg-white/20 backdrop-blur-sm transition-transform group-hover:scale-110">
              {isConnected ? (
                <CheckCircle2 className="h-5 w-5 text-white" />
              ) : (
                <ArrowRight className={`h-5 w-5 text-white transition-transform ${isSelected ? 'rotate-90' : ''}`} />
              )}
            </div>
          </div>
        </motion.button>
      </motion.div>
    )
  }

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="w-full space-y-4"
    >
      <div className="flex items-center gap-3">
        <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-gradient-to-br from-emerald-500 to-cyan-500 shadow-lg shadow-emerald-500/25">
          <Globe2 className="h-5 w-5 text-white" />
        </div>
        <div>
          <h3 className={`${quantico.className} text-base font-semibold ${isDark ? 'text-white' : 'text-slate-900'}`}>
            Fediverse
          </h3>
          <p className={`text-xs ${isDark ? 'text-white/60' : 'text-slate-500'}`}>
            Enter your handle or instance
          </p>
        </div>
      </div>

      <form
        onSubmit={(event) => {
          event.preventDefault()
          handleSubmit()
        }}
        className="space-y-4"
      >
        <div className="space-y-2">
          <label className={`${quantico.className} block text-xs font-medium uppercase tracking-wider ${isDark ? 'text-white/70' : 'text-slate-600'}`}>
            Handle or instance
          </label>
          <input
            type="text"
            value={handleText}
            onChange={(event) => {
              setHandleText(event.target.value)
              if (error) {
                validateHandle(event.target.value)
              }
            }}
            placeholder="@user@mastodon.social"
            className={`${quantico.className} w-full px-4 py-3 rounded-xl border-2 transition-all duration-200 outline-none ${inputClasses} ${error ? 'border-red-500' : ''}`}
            disabled={isLoading}
          />
        </div>

        {error && (
          <div className={`flex items-center gap-3 p-3 rounded-xl ${isDark ? 'bg-red-500/20 border border-red-500/30' : 'bg-red-50 border border-red-200'}`}>
            <AlertCircle className={`w-4 h-4 flex-shrink-0 ${isDark ? 'text-red-400' : 'text-red-500'}`} />
            <p className={`${quantico.className} text-sm ${isDark ? 'text-red-300' : 'text-red-600'}`}>
              {error}
            </p>
          </div>
        )}

        <motion.button
          type="submit"
          whileHover={{ scale: 1.01 }}
          whileTap={{ scale: 0.99 }}
          disabled={isLoading || !handleText || !!error}
          className={`${quantico.className} w-full flex items-center justify-center gap-3 px-5 py-3.5 rounded-xl font-medium transition-all duration-300 disabled:opacity-50 disabled:cursor-not-allowed bg-gradient-to-r from-emerald-500 via-teal-500 to-cyan-500 text-white shadow-lg shadow-emerald-500/25 hover:shadow-emerald-500/40`}
        >
          {isLoading ? (
            <Loader2 className="w-5 h-5 animate-spin" />
          ) : (
            <>
              <span>Connect Fediverse</span>
              <ArrowRight className="w-4 h-4" />
            </>
          )}
        </motion.button>
      </form>
    </motion.div>
  )
}
