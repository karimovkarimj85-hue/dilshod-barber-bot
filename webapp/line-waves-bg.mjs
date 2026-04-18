/**
 * LineWaves background (OGL) — vanilla port of ReactBits-style component.
 */
function hexToVec3(hex) {
  const h = hex.replace('#', '');
  return [
    parseInt(h.slice(0, 2), 16) / 255,
    parseInt(h.slice(2, 4), 16) / 255,
    parseInt(h.slice(4, 6), 16) / 255,
  ];
}

const vertexShader = `
attribute vec2 uv;
attribute vec2 position;
varying vec2 vUv;
void main() {
  vUv = uv;
  gl_Position = vec4(position, 0, 1);
}
`;

const fragmentShader = `
precision highp float;

uniform float uTime;
uniform vec3 uResolution;
uniform float uSpeed;
uniform float uInnerLines;
uniform float uOuterLines;
uniform float uWarpIntensity;
uniform float uRotation;
uniform float uEdgeFadeWidth;
uniform float uColorCycleSpeed;
uniform float uBrightness;
uniform vec3 uColor1;
uniform vec3 uColor2;
uniform vec3 uColor3;
uniform vec2 uMouse;
uniform float uMouseInfluence;
uniform float uEnableMouse;

#define HALF_PI 1.5707963

float hashF(float n) {
  return fract(sin(n * 127.1) * 43758.5453123);
}

float smoothNoise(float x) {
  float i = floor(x);
  float f = fract(x);
  float u = f * f * (3.0 - 2.0 * f);
  return mix(hashF(i), hashF(i + 1.0), u);
}

float displaceA(float coord, float t) {
  float result = sin(coord * 2.123) * 0.2;
  result += sin(coord * 3.234 + t * 4.345) * 0.1;
  result += sin(coord * 0.589 + t * 0.934) * 0.5;
  return result;
}

float displaceB(float coord, float t) {
  float result = sin(coord * 1.345) * 0.3;
  result += sin(coord * 2.734 + t * 3.345) * 0.2;
  result += sin(coord * 0.189 + t * 0.934) * 0.3;
  return result;
}

vec2 rotate2D(vec2 p, float angle) {
  float c = cos(angle);
  float s = sin(angle);
  return vec2(p.x * c - p.y * s, p.x * s + p.y * c);
}

void main() {
  vec2 coords = gl_FragCoord.xy / uResolution.xy;
  coords = coords * 2.0 - 1.0;
  coords = rotate2D(coords, uRotation);

  float halfT = uTime * uSpeed * 0.5;
  float fullT = uTime * uSpeed;

  float mouseWarp = 0.0;
  if (uEnableMouse > 0.5) {
    vec2 mPos = rotate2D(uMouse * 2.0 - 1.0, uRotation);
    float mDist = length(coords - mPos);
    mouseWarp = uMouseInfluence * exp(-mDist * mDist * 4.0);
  }

  float warpAx = coords.x + displaceA(coords.y, halfT) * uWarpIntensity + mouseWarp;
  float warpAy = coords.y - displaceA(coords.x * cos(fullT) * 1.235, halfT) * uWarpIntensity;
  float warpBx = coords.x + displaceB(coords.y, halfT) * uWarpIntensity + mouseWarp;
  float warpBy = coords.y - displaceB(coords.x * sin(fullT) * 1.235, halfT) * uWarpIntensity;

  vec2 fieldA = vec2(warpAx, warpAy);
  vec2 fieldB = vec2(warpBx, warpBy);
  vec2 blended = mix(fieldA, fieldB, mix(fieldA, fieldB, 0.5));

  float fadeTop = smoothstep(uEdgeFadeWidth, uEdgeFadeWidth + 0.4, blended.y);
  float fadeBottom = smoothstep(-uEdgeFadeWidth, -(uEdgeFadeWidth + 0.4), blended.y);
  float vMask = 1.0 - max(fadeTop, fadeBottom);

  float tileCount = mix(uOuterLines, uInnerLines, vMask);
  float scaledY = blended.y * tileCount;
  float nY = smoothNoise(abs(scaledY));

  float ridge = pow(
    step(abs(nY - blended.x) * 2.0, HALF_PI) * cos(2.0 * (nY - blended.x)),
    5.0
  );

  float lines = 0.0;
  for (float i = 1.0; i < 3.0; i += 1.0) {
    lines += pow(max(fract(scaledY), fract(-scaledY)), i * 2.0);
  }

  float pattern = vMask * lines;

  float cycleT = fullT * uColorCycleSpeed;
  float rChannel = (pattern + lines * ridge) * (cos(blended.y + cycleT * 0.234) * 0.5 + 1.0);
  float gChannel = (pattern + vMask * ridge) * (sin(blended.x + cycleT * 1.745) * 0.5 + 1.0);
  float bChannel = (pattern + lines * ridge) * (cos(blended.x + cycleT * 0.534) * 0.5 + 1.0);

  vec3 col = (rChannel * uColor1 + gChannel * uColor2 + bChannel * uColor3) * uBrightness;
  float alpha = clamp(length(col), 0.0, 1.0);

  gl_FragColor = vec4(col, alpha);
}
`;

export async function mountLineWaves(container) {
  if (!container) return () => {};
  let mod;
  try {
    mod = await import('https://esm.sh/ogl');
  } catch (e) {
    console.warn('[LineWaves] OGL load failed', e);
    return () => {};
  }
  const { Renderer, Program, Mesh, Triangle } = mod;

  const renderer = new Renderer({ alpha: true, premultipliedAlpha: false, dpr: Math.min(window.devicePixelRatio || 1, 2) });
  const gl = renderer.gl;
  gl.clearColor(0, 0, 0, 0);

  const speed = 0.3;
  const innerLineCount = 32;
  const outerLineCount = 36;
  const warpIntensity = 1;
  const rotation = -45;
  const edgeFadeWidth = 0;
  const colorCycleSpeed = 1;
  const brightness = 0.22;
  const color1 = '#ffffff';
  const color2 = '#ffffff';
  const color3 = '#ffffff';
  const mouseInfluence = 2;
  const enableMouse = true;

  let program;
  let currentMouse = [0.5, 0.5];
  let targetMouse = [0.5, 0.5];

  function clamp01(x) {
    return Math.max(0, Math.min(1, x));
  }

  function updatePointerFromEvent(e) {
    const rect = gl.canvas.getBoundingClientRect();
    const cx = e.touches ? e.touches[0].clientX : e.clientX;
    const cy = e.touches ? e.touches[0].clientY : e.clientY;
    if (!rect.width || !rect.height) return;
    targetMouse = [clamp01((cx - rect.left) / rect.width), clamp01(1 - (cy - rect.top) / rect.height)];
  }

  function resize() {
    const w = container.offsetWidth || window.innerWidth;
    const h = container.offsetHeight || window.innerHeight;
    renderer.setSize(w, h);
    if (program) {
      const cw = gl.canvas.width;
      const ch = gl.canvas.height;
      program.uniforms.uResolution.value = [cw, ch, cw / Math.max(ch, 1)];
    }
  }

  const rotationRad = (rotation * Math.PI) / 180;
  const geometry = new Triangle(gl);
  program = new Program(gl, {
    vertex: vertexShader,
    fragment: fragmentShader,
    uniforms: {
      uTime: { value: 0 },
      uResolution: { value: [1, 1, 1] },
      uSpeed: { value: speed },
      uInnerLines: { value: innerLineCount },
      uOuterLines: { value: outerLineCount },
      uWarpIntensity: { value: warpIntensity },
      uRotation: { value: rotationRad },
      uEdgeFadeWidth: { value: edgeFadeWidth },
      uColorCycleSpeed: { value: colorCycleSpeed },
      uBrightness: { value: brightness },
      uColor1: { value: hexToVec3(color1) },
      uColor2: { value: hexToVec3(color2) },
      uColor3: { value: hexToVec3(color3) },
      uMouse: { value: new Float32Array([0.5, 0.5]) },
      uMouseInfluence: { value: mouseInfluence },
      uEnableMouse: { value: enableMouse ? 1 : 0 },
    },
  });

  const mesh = new Mesh(gl, { geometry, program });
  gl.canvas.style.width = '100%';
  gl.canvas.style.height = '100%';
  gl.canvas.style.display = 'block';
  container.appendChild(gl.canvas);

  window.addEventListener('resize', resize);
  window.addEventListener('pointermove', updatePointerFromEvent, { passive: true });
  window.addEventListener('pointerdown', updatePointerFromEvent, { passive: true });
  window.addEventListener('touchmove', updatePointerFromEvent, { passive: true });

  resize();

  let animationFrameId;
  function update(time) {
    animationFrameId = requestAnimationFrame(update);
    program.uniforms.uTime.value = time * 0.001;
    currentMouse[0] += 0.06 * (targetMouse[0] - currentMouse[0]);
    currentMouse[1] += 0.06 * (targetMouse[1] - currentMouse[1]);
    program.uniforms.uMouse.value[0] = currentMouse[0];
    program.uniforms.uMouse.value[1] = currentMouse[1];
    try {
      renderer.render({ scene: mesh });
    } catch (_) {}
  }
  animationFrameId = requestAnimationFrame(update);

  return () => {
    cancelAnimationFrame(animationFrameId);
    window.removeEventListener('resize', resize);
    window.removeEventListener('pointermove', updatePointerFromEvent);
    window.removeEventListener('touchmove', updatePointerFromEvent);
    try {
      if (gl.canvas && gl.canvas.parentNode) gl.canvas.parentNode.removeChild(gl.canvas);
      gl.getExtension('WEBGL_lose_context')?.loseContext();
    } catch (_) {}
  };
}

const root = document.getElementById('line-waves-root');
if (root) {
  mountLineWaves(root).catch((e) => console.warn('[LineWaves]', e));
}
