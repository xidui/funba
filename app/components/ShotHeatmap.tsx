import { View } from "react-native";
import Svg, { Circle, Line, Path, Rect } from "react-native-svg";
import { court } from "../lib/theme";

type Dot = { x: number; y: number; made: boolean };

const COURT_WIDTH = 500;   // -250..250 in NBA units
const COURT_HEIGHT = 470;  // -47..423

/** NBA shot coordinates are centered at the basket; y grows away from the hoop. */
function project(x: number, y: number, width: number, height: number): { cx: number; cy: number } {
  const cx = ((x + 250) / COURT_WIDTH) * width;
  const cy = ((y + 47) / COURT_HEIGHT) * height;
  return { cx, cy };
}

export function ShotHeatmap({ dots, zones }: { dots: Dot[]; zones: Record<string, any> }) {
  const width = 320;
  const height = Math.round(width * (COURT_HEIGHT / COURT_WIDTH));
  return (
    <View className="items-center">
      <Svg width={width} height={height}>
        <Rect x={0} y={0} width={width} height={height} fill={court.bg} />
        {/* Halfcourt outline */}
        <Rect x={1} y={1} width={width - 2} height={height - 2} fill="none" stroke={court.line} strokeWidth={1.5} />
        {/* Free throw line area */}
        <Rect x={width * 0.36} y={0} width={width * 0.28} height={height * 0.40} fill="none" stroke={court.line} strokeWidth={1.2} />
        {/* Restricted area arc */}
        <Path d={`M ${width * 0.42} 0 A ${width * 0.08} ${width * 0.08} 0 0 0 ${width * 0.58} 0`} fill="none" stroke={court.line} strokeWidth={1.2} />
        {/* 3-point line — simplified */}
        <Path
          d={`M ${width * 0.075} 0 L ${width * 0.075} ${height * 0.30} A ${width * 0.425} ${width * 0.425} 0 0 0 ${width * 0.925} ${height * 0.30} L ${width * 0.925} 0`}
          fill="none"
          stroke={court.line}
          strokeWidth={1.2}
        />
        {/* Basket */}
        <Circle cx={width / 2} cy={height * 0.02} r={4} fill={court.line} />
        {/* Shot dots */}
        {dots.slice(0, 1500).map((d, i) => {
          const { cx, cy } = project(d.x, d.y, width, height);
          return (
            <Circle
              key={i}
              cx={cx}
              cy={height - cy /* flip: hoop at top */}
              r={2.2}
              fill={d.made ? "#f97316" : "#7d8a9c"}
              fillOpacity={0.6}
            />
          );
        })}
      </Svg>
    </View>
  );
}
