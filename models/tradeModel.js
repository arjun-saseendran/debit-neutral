import { Schema, model } from "mongoose";

const schema = new Schema(
  {
    index: { type: String, default: "SENSEX" },
    status: {
      type: String,
      enum: ["ACTIVE", "EXITING", "COMPLETED"],
      default: "ACTIVE",
    },

    instrumentKeys: {
      callBuy: String,
      callSell: String,
      putBuy: String,
      putSell: String,
    },

    symbols: {
      callBuy: String,
      callSell: String,
      putBuy: String,
      putSell: String,
    },

    strikes: {
      callBuy: Number,
      callSell: Number,
      putBuy: Number,
      putSell: Number,
    },

    entryPremiums: {
      callBuy: { type: Number, default: 0 },
      callSell: { type: Number, default: 0 },
      putBuy: { type: Number, default: 0 },
      putSell: { type: Number, default: 0 },
    },

    totalPremiumPaid: { type: Number, default: 0 },

    quantity: { type: Number, required: true },
    expiry: { type: String },

    legsAlive: {
      callBuy: { type: Boolean, default: true },
      callSell: { type: Boolean, default: true },
      putBuy: { type: Boolean, default: true },
      putSell: { type: Boolean, default: true },
    },

    lockedProfit: { type: Number, default: 0 },
    peakProfit: { type: Number, default: 0 },
    trailActive: { type: Boolean, default: false },

    enteredAt: { type: Date, default: Date.now },
  },
  { timestamps: true },
);

let _model = null;
const getTradeModel = () => {
  if (!_model) _model = model("Trade", schema);
  return _model;
};
export default getTradeModel;