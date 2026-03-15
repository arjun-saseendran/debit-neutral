import { Schema, model } from "mongoose";

const schema = new Schema(
  {
    strategy: { type: String, default: "WEEKEND_STRANGLE" },
    activeTradeId: {
      type: Schema.Types.ObjectId,
      ref: "WeekendStrangle",
    },
    index: { type: String, default: "SENSEX" },
    realizedPnL: { type: Number, default: 0 },
    exitReason: {
      type: String,
      enum: [
        "TRAIL_HIT",
        "OVERALL_SL",
        "CALL_BUY_SL",
        "PUT_BUY_SL",
        "CALL_SELL_SL",
        "PUT_SELL_SL",
        "EOD_EXIT",
        "MANUAL_EXIT",
      ],
    },
    notes: String,
  },
  { timestamps: true },
);

let _model = null;
export const getTradePerformanceModel = () => {
  if (!_model) _model = model("TradePerformance", schema);
  return _model;
};
