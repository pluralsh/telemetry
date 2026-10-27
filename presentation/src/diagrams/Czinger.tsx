export function Czinger() {
  return (
    <figure className="flex h-full flex-col">
      <div className="relative flex-1 overflow-hidden border border-line-strong bg-panel">
        <span aria-hidden className="node" style={{ left: -4.5, top: -4.5 }} />
        <span aria-hidden className="node" style={{ right: -4.5, top: -4.5 }} />
        <span aria-hidden className="node" style={{ left: -4.5, bottom: -4.5 }} />
        <span aria-hidden className="node" style={{ right: -4.5, bottom: -4.5 }} />
        <img src="./images/czinger-21c-chassis.jpg" alt="Czinger 21C 3D-printed rear suspension and subframe" className="absolute inset-0 h-full w-full object-cover object-[50%_40%]" />
      </div>
      <figcaption className="code mt-3 text-[14px] text-faint">Czinger 21C rear subframe · Prova MO, CC BY-SA 4.0, via Wikimedia Commons</figcaption>
    </figure>
  );
}
