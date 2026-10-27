import { createRoot } from "react-dom/client";
import "@fontsource/newsreader/400.css";
import "@fontsource/newsreader/400-italic.css";
import "./styles.css";
import { Deck } from "./deck/Deck";
import { SLIDES } from "./slides";

createRoot(document.getElementById("root")!).render(<Deck slides={SLIDES} />);
