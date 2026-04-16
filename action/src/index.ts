// Thin wrapper around the `niks3 ci` subcommands. All orchestration logic
// (fetching cache config, writing nix.conf, starting the daemon, draining)
// lives in the Go binary — this file's only jobs are downloading that binary
// and giving it the main/post lifecycle that composite actions can't have.

import * as core from '@actions/core'
import * as tc from '@actions/tool-cache'
import { spawnSync } from 'node:child_process'
import * as os from 'node:os'
import * as path from 'node:path'

// Baked at bundle time via esbuild --define. Matches the git tag, which
// matches the goreleaser release name — so `Mic92/niks3@v1.5.0` downloads
// `niks3_<plat>.tar.gz` from the v1.5.0 release.
declare const NIKS3_VERSION: string

const isPost = !!core.getState('isPost')

async function main(): Promise<void> {
  if (isPost) {
    await post()
  } else {
    core.saveState('isPost', 'true')
    await setup()
  }
}

async function setup(): Promise<void> {
  const bin = await resolveBinary()
  core.saveState('bin', bin)

  // `niks3 ci setup` reads action inputs directly from INPUT_* env vars
  // (server-url, substituter, public-key, audience, skip-push, debug)
  // so we only pass structural flags here.
  const r = spawnSync(bin, ['ci', 'setup'], { stdio: 'inherit' })

  if (r.error) {
    throw r.error
  }

  if (r.signal) {
    throw new Error(`niks3 ci setup killed by ${r.signal}`)
  }
  if (r.status !== 0) {
    throw new Error(`niks3 ci setup exited ${r.status}`)
  }
}

async function post(): Promise<void> {
  const bin = core.getState('bin')
  if (!bin) {
    // setup never ran (or failed before saving state) — nothing to drain.
    return
  }

  const timeoutSec = core.getInput('drain-timeout') || '600'

  const r = spawnSync(
    bin,
    ['ci', 'stop', '--timeout', `${timeoutSec}s`],
    { stdio: 'inherit' },
  )

  if (r.error) {
    core.warning(`niks3 ci stop failed to spawn: ${r.error.message}`)
  }

  // Never throw here — push failures are cache misses, not CI gates.
  // `ci stop` itself emits ::warning:: on problems.
}

async function resolveBinary(): Promise<string> {
  const override = core.getInput('niks3-bin')
  if (override) {
    core.info(`Using niks3 binary from input: ${override}`)
    return override
  }

  const plat = platformTuple()
  const cached = tc.find('niks3', NIKS3_VERSION, plat)

  if (cached) {
    core.info(`Found cached niks3 ${NIKS3_VERSION} (${plat})`)
    return path.join(cached, 'niks3')
  }

  const url = `https://github.com/Mic92/niks3/releases/download/${NIKS3_VERSION}/niks3_${plat}.tar.gz`

  core.info(`Downloading niks3 ${NIKS3_VERSION} from ${url}`)

  const tarball = await tc.downloadTool(url)
  const extracted = await tc.extractTar(tarball)
  const dir = await tc.cacheDir(extracted, 'niks3', NIKS3_VERSION, plat)

  return path.join(dir, 'niks3')
}

// platformTuple returns the goreleaser archive suffix (e.g. "Linux_x86_64").
// Matches the name_template in .goreleaser.yaml.
function platformTuple(): string {
  const sys: Record<string, string> = {
    linux: 'Linux',
    darwin: 'Darwin',
  }

  const arch: Record<string, string> = {
    x64: 'x86_64',
    arm64: 'arm64',
  }

  const s = sys[os.platform()]
  const a = arch[os.arch()]

  if (!s || !a) {
    throw new Error(`unsupported platform: ${os.platform()}/${os.arch()}`)
  }

  return `${s}_${a}`
}

main().catch((err: Error) => {
  if (isPost) {
    core.warning(`post step failed: ${err.message}`)
  } else {
    core.setFailed(err.message)
  }
})
