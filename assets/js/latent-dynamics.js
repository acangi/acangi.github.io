(() => {
  "use strict";

  const stage = document.getElementById("latent-dynamics");
  if (!stage) return;
  const canvas = stage.querySelector("canvas");
  const ctx = canvas.getContext("2d", { alpha: true });
  if (!ctx) return;
  const tooltip = stage.querySelector('[role="tooltip"]');
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");
  const N = stage.clientWidth < 600 ? 240 : 420;

  // Adapted from the supplied latent-space dynamics study. This is synthetic data.
  // The physical force model and fixed PCA map are independent of the camera.
  const K = 2.55, smoothing = 0.37;
  // Thermal fluctuations around three minima give each basin a three-dimensional volume.
  const well = [ [-1.28,-0.78,-0.92], [1.30,-0.69,0.91], [0.03,1.34,0.19] ];
  const clamp=(x,a,b)=>Math.max(a,Math.min(b,x));
  const lerp=(a,b,t)=>a+(b-a)*t;
  let seed=289733;  // deterministic initial ensemble
  function rand(){seed=(Math.imul(1664525,seed)+1013904223)>>>0;return seed/4294967296;}
  let spare = null;
  function gaussian(){
    if(spare !== null){const v=spare;spare=null;return v;}
    const a=Math.max(rand(),1e-12), b=rand(), m=Math.sqrt(-2*Math.log(a));
    spare=m*Math.sin(2*Math.PI*b);
    return m*Math.cos(2*Math.PI*b);
  }
  function basin(q){
    let best=0, dist=Infinity;
    for(let i=0;i<3;i++){
      const w=well[i], dx=q[0]-w[0],dy=q[1]-w[1],dz=q[2]-w[2];
      const d=dx*dx+dy*dy+dz*dz;
      if(d<dist){dist=d;best=i;}
    }
    return best;
  }
  function grad(q,field){
    // U0(q) = -eps log sum_j exp( -(K/2)|q-c_j|^2 / eps )
    // D U0 = K * [q - sum_j w_j c_j] with softmax weights w_j.
    const e=[];let min=1e10;
    for(let j=0;j<3;j++){
      const c=well[j], dx=q[0]-c[0],dy=q[1]-c[1],dz=q[2]-c[2];
      e[j]=K*.5*(dx*dx+dy*dy+dz*dz);
      min=Math.min(min,e[j]);
    }
    let s=0;
    for(let j=0;j<3;j++){e[j]=Math.exp((min-e[j])/smoothing);s+=e[j];}
    let cx=0,cy=0,cz=0;
    for(let j=0;j<3;j++){const w=e[j]/s;cx+=w*well[j][0];cy+=w*well[j][1];cz+=w*well[j][2];}
    return [K*(q[0]-cx)-field,K*(q[1]-cy),K*(q[2]-cz)];
  }
  // Nonlinear feature map. PCA determines its three principal axes from reference configurations.
  const D=12;
  function descriptor(q){
    const [x,y,z]=q;
    return [x*.60,y*.60,z*1.08,Math.sin(1.25*x),Math.cos(1.25*x),Math.sin(1.22*y),Math.cos(1.22*y),
      .24*x*y,.20*(x*x-y*y),.36*z*x,.36*z*y, .18*(x*x+y*y+z*z)];
  }
  function dot(a,b){let s=0;for(let i=0;i<a.length;i++)s+=a[i]*b[i];return s;}
  function normalized(v){const n=Math.hypot(...v)||1;return v.map(x=>x/n);}
  const train = [];
  for(let i=0;i<2100;i++){
    const c=well[i%3],t=.53;
    let q=[0,1,2].map(j=>c[j]+Math.sqrt(t/K)*gaussian());
    for(let s=0;s<6;s++){
      const g=grad(q,0), n=Math.sqrt(2*t*.025);
      q=[q[0]-g[0]*.025+n*gaussian(),q[1]-g[1]*.025+n*gaussian(),q[2]-g[2]*.025+n*gaussian()];
    }
    train.push(descriptor(q));
  }
  const mean=Array(D).fill(0),cov=Array.from({length:D},()=>Array(D).fill(0));
  for(const f of train)for(let j=0;j<D;j++)mean[j]+=f[j]/train.length;
  for(const f of train){
    for(let j=0;j<D;j++)for(let k=j;k<D;k++)cov[j][k]+=(f[j]-mean[j])*(f[k]-mean[k])/train.length;
  }
  for(let j=0;j<D;j++)for(let k=0;k<j;k++)cov[j][k]=cov[k][j];
  const matvec=v=>cov.map(row=>dot(row,v));
  const basis=[];
  for(let a=0;a<3;a++){
    let v=normalized(Array.from({length:D},(_,j)=>Math.sin((a+2)*(j+1)*1.91)));
    for(let iteration=0;iteration<130;iteration++){
      let u=matvec(v);
      for(const b of basis){const pr=dot(u,b);u=u.map((x,i)=>x-pr*b[i]);}
      v=normalized(u);
    }
    basis.push(v);
  }
  // Force an orientation where the same random seed always produces the same artwork.
  function rawEmbed(q){const d=descriptor(q).map((x,j)=>x-mean[j]);return basis.map(b=>dot(d,b));}
  const cloud=train.map(f=>{
    const d=f.map((x,j)=>x-mean[j]);return basis.map(b=>dot(d,b));
  });
  const extents=[];
  for(let j=0;j<3;j++){
    const v=cloud.map(p=>p[j]).sort((a,b)=>a-b);
    const lo=v[Math.floor(.012*v.length)],hi=v[Math.floor(.988*v.length)];
    extents.push({mid:(lo+hi)*.5, half:Math.max(.60,(hi-lo)*.5)});
  }
  function embed(q){
    const v=rawEmbed(q);
    return [
      (v[0]-extents[0].mid)/extents[0].half*1.45,
      (v[1]-extents[1].mid)/extents[1].half*1.23,
      (v[2]-extents[2].mid)/extents[2].half*1.55
    ];
  }

  let width = 0, height = 0;
  let sprites = [], colors = [];
  const particles = Array.from({ length: N }, (_, i) => {
    const center = well[i % 3], amplitude = Math.sqrt(.43 / K);
    const q = center.map(x => x + amplitude * gaussian());
    return {
      q, v: center.map(() => Math.sqrt(.43) * gaussian()), z: embed(q),
      b: i % 3, size: .85 + rand() * .45, track: i % 23 === 0, trail: []
    };
  });

  // Cache lit sphere textures: perspective and occlusion then communicate volume,
  // while the light stays fixed in screen space as the camera moves.
  function readPalette() {
    const style = getComputedStyle(stage);
    colors = ["primary", "secondary", "tertiary"].map(name => {
      ctx.fillStyle = style.getPropertyValue(`--latent-${name}`).trim();
      const value = ctx.fillStyle;
      return value.startsWith("#")
        ? [1, 3, 5].map(i => parseInt(value.slice(i, i + 2), 16))
        : value.match(/[\d.]+/g).slice(0, 3).map(Number);
    });
    sprites = colors.map(rgb => {
      const sprite = document.createElement("canvas");
      sprite.width = sprite.height = 64;
      const surface = sprite.getContext("2d");
      const shade = (white, brightness = 1) => `rgb(${rgb.map(c => Math.round(lerp(c * brightness, 255, white))).join(",")})`;
      const light = surface.createRadialGradient(23, 20, 1, 32, 32, 31);
      light.addColorStop(0, shade(.72));
      light.addColorStop(.24, shade(.27));
      light.addColorStop(.56, shade(0));
      light.addColorStop(1, shade(0, .42));
      surface.fillStyle = light;
      surface.beginPath();
      surface.arc(32, 32, 30, 0, Math.PI * 2);
      surface.fill();
      return sprite;
    });
  }

  let field = 0, temperature = .43;
  let cameraX = 0, cameraY = 0;
  const pointer = { x: 0, y: 0, active: false };
  const friction = .88, stepSize = 1 / 120;
  let steps = 0;
  function physicalStep(dt) {
    // BAOAB integration of dq = v dt,
    // dv = [-grad U(q) + F e_x - gamma v]dt + sqrt(2 gamma kBT)dW.
    const decay = Math.exp(-friction * dt);
    const kick = Math.sqrt(temperature * (1 - decay * decay));
    for (const p of particles) {
      let force = grad(p.q, field);
      for (let j = 0; j < 3; j++) {
        p.v[j] -= .5 * dt * force[j];
        p.q[j] += .5 * dt * p.v[j];
        p.v[j] = decay * p.v[j] + kick * gaussian();
        p.q[j] += .5 * dt * p.v[j];
      }
      force = grad(p.q, field);
      for (let j = 0; j < 3; j++) p.v[j] -= .5 * dt * force[j];
      p.z = embed(p.q);
      p.b = basin(p.q);
      if (p.track && steps % 2 === 0) {
        // Store the basin with each sample, so old trail colors remain truthful.
        p.trail.push({ z: p.z.slice(), b: p.b });
        if (p.trail.length > 48) p.trail.shift();
      }
    }
    steps++;
  }
  // Start with short, physically integrated histories rather than empty trails.
  for (let i = 0; i < 64; i++) physicalStep(stepSize);

  let clock = 0;
  function project(v, yaw, pitch) {
    const cy = Math.cos(yaw), sy = Math.sin(yaw);
    const cp = Math.cos(pitch), sp = Math.sin(pitch);
    const x = cy * v[0] + sy * v[2];
    const z = -sy * v[0] + cy * v[2];
    const y = cp * v[1] - sp * z;
    const depth = sp * v[1] + cp * z;
    const perspective = 6 / Math.max(1.8, 6 - depth);
    const scale = Math.min(width * .205, height * .245);
    // A slight roll avoids a flat, horizontal arrangement of the three basins.
    const roll = -.12;
    return {
      x: width * .5 + (x * Math.cos(roll) - y * Math.sin(roll)) * scale * perspective,
      y: height * .5 - (x * Math.sin(roll) + y * Math.cos(roll)) * scale * perspective,
      depth, perspective
    };
  }

  function render() {
    if (!width || !height || !sprites.length) return;
    ctx.clearRect(0, 0, width, height);
    const yaw = .65 + clock * .18 + cameraX * .5;
    const pitch = .32 + Math.sin(clock * .14) * .3 + cameraY * .26;
    const scene = [];
    for (const p of particles) {
      const point = project(p.z, yaw, pitch);
      scene.push({ point, p, depth: point.depth });
      for (let i = 1; i < p.trail.length; i++) {
        const a = project(p.trail[i - 1].z, yaw, pitch);
        const b = project(p.trail[i].z, yaw, pitch);
        scene.push({ a, b, basin: p.trail[i].b, fade: i / p.trail.length, depth: (a.depth + b.depth) / 2 });
      }
    }
    // Sort trajectory segments together with particles for correct occlusion.
    scene.sort((a, b) => a.depth - b.depth);
    ctx.lineCap = "round";
    for (const item of scene) {
      const near = clamp((item.depth + 2.5) / 5, 0, 1);
      if (item.p) {
        const { p, point } = item;
        const radius = clamp(p.size * (2.7 + 1.8 * near) * point.perspective * (width < 600 ? .88 : 1), 1.4, 13);
        if (point.x < -radius || point.x > width + radius || point.y < -radius || point.y > height + radius) continue;
        ctx.globalAlpha = .35 + .65 * near;
        ctx.drawImage(sprites[p.b], point.x - radius, point.y - radius, radius * 2, radius * 2);
      } else {
        ctx.globalAlpha = Math.pow(item.fade, 1.3) * (.14 + .36 * near);
        ctx.strokeStyle = `rgb(${colors[item.basin].join(",")})`;
        ctx.lineWidth = (.8 + near * .9) * item.b.perspective;
        ctx.beginPath();
        ctx.moveTo(item.a.x, item.a.y);
        ctx.lineTo(item.b.x, item.b.y);
        ctx.stroke();
      }
    }
    ctx.globalAlpha = 1;
  }

  let hoverTimer = null, keyboardFocus = false, dismissed = false;
  function positionTooltip() {
    const bounds = stage.getBoundingClientRect();
    const gap = 20, edge = 16, minimumSideWidth = 220;
    const rightRoom = document.documentElement.clientWidth - bounds.right - edge;
    const leftRoom = bounds.left - edge;
    const side = rightRoom >= minimumSideWidth + gap ? "right" : leftRoom >= minimumSideWidth + gap ? "left" : "below";
    const availableWidth = side === "right" ? rightRoom - gap : leftRoom - gap;
    tooltip.style.width = `${side === "below" ? Math.min(350, width) : Math.min(300, availableWidth)}px`;
    tooltip.setAttribute("data-placement", side);
    // Dock outside the canvas, independently of cursor position.
    const left = side === "right" ? width + gap : side === "left" ? -tooltip.offsetWidth - gap : (width - tooltip.offsetWidth) / 2;
    const top = side === "below" ? height + gap : Math.max(0, (height - tooltip.offsetHeight) / 2);
    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
    // Reserve room for a description below the canvas, including tall side cards.
    const clearance = tooltip.hasAttribute("data-visible") ? Math.max(0, top + tooltip.offsetHeight - height) : 0;
    stage.style.setProperty("--latent-description-space", `${clearance}px`);
  }
  function hideTooltip() {
    clearTimeout(hoverTimer);
    hoverTimer = null;
    tooltip.removeAttribute("data-visible");
    stage.style.removeProperty("--latent-description-space");
  }
  function queueTooltip() {
    if (dismissed || hoverTimer !== null || tooltip.hasAttribute("data-visible")) return;
    hoverTimer = setTimeout(() => {
      hoverTimer = null;
      if ((!pointer.active && !keyboardFocus) || dismissed) return;
      tooltip.setAttribute("data-visible", "");
      positionTooltip();
    }, 1500);
  }
  function setPointer(event) {
    if (event.pointerType === "touch") return;
    const rect = stage.getBoundingClientRect();
    pointer.x = clamp((event.clientX - rect.left) / rect.width * 2 - 1, -1, 1);
    pointer.y = clamp((event.clientY - rect.top) / rect.height * 2 - 1, -1, 1);
    pointer.active = true;
    queueTooltip();
  }
  stage.addEventListener("pointerenter", setPointer, { passive: true });
  stage.addEventListener("pointermove", setPointer, { passive: true });
  function leavePointer() {
    pointer.active = false;
    dismissed = false;
    if (!keyboardFocus) hideTooltip();
  }
  stage.addEventListener("pointerleave", leavePointer, { passive: true });
  stage.addEventListener("pointercancel", leavePointer, { passive: true });
  stage.addEventListener("focus", () => {
    keyboardFocus = stage.matches(":focus-visible");
    dismissed = false;
    if (keyboardFocus) queueTooltip();
  });
  stage.addEventListener("blur", () => {
    keyboardFocus = false;
    dismissed = false;
    hideTooltip();
  });
  stage.addEventListener("keydown", event => {
    if (event.key === "Escape") {
      dismissed = true;
      hideTooltip();
    }
  });

  let inView = !("IntersectionObserver" in window);
  let animationFrame = null, last = null, accumulator = 0;
  function start() {
    if (animationFrame === null && inView && !document.hidden && !reducedMotion.matches) {
      last = null;
      animationFrame = requestAnimationFrame(tick);
    }
  }
  function stop() {
    if (animationFrame !== null) cancelAnimationFrame(animationFrame);
    animationFrame = null;
    last = null;
    accumulator = 0;
  }
  function tick(now) {
    animationFrame = null;
    if (!inView || document.hidden || reducedMotion.matches) return;
    const delta = last === null ? 0 : Math.min(.05, (now - last) / 1000);
    last = now;
    const response = 1 - Math.exp(-delta * 1.2);
    field = lerp(field, pointer.active ? pointer.x * .87 : 0, response);
    temperature = lerp(temperature, pointer.active ? lerp(.18, .95, (pointer.y + 1) / 2) : .43, response);
    cameraX = lerp(cameraX, pointer.active ? pointer.x : 0, response);
    cameraY = lerp(cameraY, pointer.active ? pointer.y : 0, response);
    clock += delta;
    // Fixed model steps make noise, trajectories, and speed independent of refresh rate.
    accumulator += delta * .24;
    while (accumulator >= stepSize) {
      physicalStep(stepSize);
      accumulator -= stepSize;
    }
    render();
    animationFrame = requestAnimationFrame(tick);
  }
  function resize() {
    const bounds = stage.getBoundingClientRect();
    width = Math.max(1, bounds.width);
    height = Math.max(1, bounds.height);
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    canvas.width = Math.round(width * dpr);
    canvas.height = Math.round(height * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    positionTooltip();
    render();
  }
  readPalette();
  resize();
  if ("ResizeObserver" in window) new ResizeObserver(resize).observe(stage);
  // Viewport changes can move the centered canvas without changing its size.
  window.addEventListener("resize", resize);
  new MutationObserver(() => {
    readPalette();
    render();
  }).observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
  if ("IntersectionObserver" in window) {
    new IntersectionObserver(entries => {
      inView = entries[0].isIntersecting;
      if (inView) start();
      else { stop(); hideTooltip(); }
    }, { rootMargin: "80px" }).observe(stage);
  }
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) { stop(); hideTooltip(); }
    else start();
  });
  reducedMotion.addEventListener("change", () => {
    stop();
    render();
    start();
  });
  start();
})();

