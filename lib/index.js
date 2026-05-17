"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
  function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
  return new (P || (P = Promise))(function (resolve, reject) {
    function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
    function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
    function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
    step((generator = generator.apply(thisArg, _arguments || [])).next());
  });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
  var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
  return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
  function verb(n) { return function (v) { return step([n, v]); }; }
  function step(op) {
    if (f) throw new TypeError("Generator is already executing.");
    while (_) try {
      if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
      if (y = 0, t) op = [op[0] & 2, t.value];
      switch (op[0]) {
        case 0: case 1: t = op; break;
        case 4: _.label++; return { value: op[1], done: false };
        case 5: _.label++; y = op[1]; op = [0]; continue;
        case 7: op = _.ops.pop(); _.trys.pop(); continue;
        default:
          if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
          if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
          if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
          if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
          if (t[2]) _.ops.pop();
          _.trys.pop(); continue;
      }
      op = body.call(thisArg, _);
    } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
    if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
  }
};
Object.defineProperty(exports, "__esModule", { value: true });
function buildMap(attrs) {
  var mp = {};
  var ks = Object.keys(attrs);
  for (var _i = 0, ks_1 = ks; _i < ks_1.length; _i++) {
    var k = ks_1[_i];
    var attr = attrs[k];
    attr.name = k;
    var field = attr.column ? attr.column : k;
    var s = field.toLowerCase();
    if (s !== k) {
      mp[s] = k;
    }
  }
  return mp;
}
exports.buildMap = buildMap;
var SqlRateRepository = (function () {
  function SqlRateRepository(db, table, attributes, max, infoTable, buildToInsert, buildToUpdate, generateId, rateIdField, rateField, count, score, authorCol, id, idField, idCol, rateCol) {
    this.db = db;
    this.table = table;
    this.attributes = attributes;
    this.max = max;
    this.infoTable = infoTable;
    this.buildToInsert = buildToInsert;
    this.buildToUpdate = buildToUpdate;
    this.generateId = generateId;
    this.rateIdField = rateIdField;
    this.map = buildMap(attributes);
    this.id = (id && id.length > 0 ? id : 'id');
    this.rate = (rateCol && rateCol.length > 0 ? rateCol : 'rate');
    this.count = (count && count.length > 0 ? count : 'count');
    this.score = (score && score.length > 0 ? score : 'score');
    this.idField = (idField && idField.length > 0 ? idField : 'id');
    this.rateField = (rateField && rateField.length > 0 ? rateField : 'rate');
    this.authorCol = (authorCol && authorCol.length > 0 ? authorCol : 'author');
    if (idCol && idCol.length > 0) {
      this.idCol = idCol;
    }
    else {
      var c = attributes[this.idField];
      if (c) {
        this.idCol = (c.column && c.column.length > 0 ? c.column : this.idField);
      }
      else {
        this.idCol = this.idField;
      }
    }
    if (rateCol && rateCol.length > 0) {
      this.rate = rateCol;
    }
    else {
      var c = attributes[this.rateField];
      if (c) {
        this.rate = (c.column && c.column.length > 0 ? c.column : this.rateField);
      }
      else {
        this.rate = this.rateField;
      }
    }
    this.load = this.load.bind(this);
    this.create = this.create.bind(this);
    this.update = this.update.bind(this);
    this.insertInfo = this.insertInfo.bind(this);
    this.updateNewInfo = this.updateNewInfo.bind(this);
    this.updateOldInfo = this.updateOldInfo.bind(this);
  }
  SqlRateRepository.prototype.load = function (id, author, tx) {
    var db = tx ? tx : this.db;
    return db.query("select * from " + this.table + " where " + this.idCol + " = " + this.db.param(1) + " and " + this.authorCol + " = " + this.db.param(2), [id, author], this.map).then(function (rates) {
      return rates && rates.length > 0 ? rates[0] : null;
    });
  };
  SqlRateRepository.prototype.create = function (rate, newInfo, tx) {
    rate[this.rateIdField] = this.generateId();
    var stmt = this.buildToInsert(rate, this.table, this.attributes, this.db.param);
    if (stmt.query) {
      var obj = rate;
      var rateNum = obj[this.rateField];
      var id = obj[this.idField];
      var db = tx ? tx : this.db;
      if (newInfo) {
        var query = this.insertInfo(rateNum);
        var s2 = { query: query, params: [id] };
        return db.executeBatch([s2, stmt], true);
      }
      else {
        var query = this.updateNewInfo(rateNum);
        var s2 = { query: query, params: [id] };
        return db.executeBatch([s2, stmt], true);
      }
    }
    else {
      return Promise.resolve(-1);
    }
  };
  SqlRateRepository.prototype.insertInfo = function (r) {
    var rateCols = [];
    var ps = [];
    for (var i = 1; i <= this.max; i++) {
      rateCols.push("" + this.rate + i);
      if (i === r) {
        ps.push('' + 1);
      }
      else {
        ps.push('0');
      }
    }
    var query = "\n    insert into " + this.infoTable + " (" + this.id + ", " + this.rate + ", " + this.count + ", " + this.score + ", " + rateCols.join(',') + ")\n    values (" + this.db.param(1) + ", " + r + ", 1, " + r + ", " + ps.join(',') + ")";
    return query;
  };
  SqlRateRepository.prototype.update = function (rate, oldRate, tx) {
    var stmt = this.buildToUpdate(rate, this.table, this.attributes, this.db.param);
    if (stmt.query) {
      var obj = rate;
      var rateNum = obj[this.rateField];
      var id = obj[this.idField];
      var query = this.updateOldInfo(rateNum, oldRate);
      var s2 = { query: query, params: [id] };
      var db = tx ? tx : this.db;
      return db.executeBatch([s2, stmt], true);
    }
    else {
      return Promise.resolve(-1);
    }
  };
  SqlRateRepository.prototype.updateNewInfo = function (r) {
    var query = "\n    update " + this.infoTable + " set " + this.rate + " = (" + this.score + " + " + r + ")/(" + this.count + " + 1), " + this.count + " = " + this.count + " + 1, " + this.score + " = " + this.score + " + " + r + ", " + this.rate + r + " = " + this.rate + r + " + 1\n    where " + this.id + " = " + this.db.param(1);
    return query;
  };
  SqlRateRepository.prototype.updateOldInfo = function (newRate, oldRate) {
    if (newRate === oldRate) {
      return '';
    }
    var delta = newRate - oldRate;
    var query = "\n    update " + this.infoTable + " set " + this.rate + " = (" + this.score + " + (" + delta + "))/" + this.count + ", " + this.score + " = " + this.score + " + (" + delta + "), " + this.rate + newRate + " = " + this.rate + newRate + " + 1, " + this.rate + oldRate + " = " + this.rate + oldRate + " - 1\n    where " + this.id + " = " + this.db.param(1);
    return query;
  };
  return SqlRateRepository;
}());
exports.SqlRateRepository = SqlRateRepository;
var Rater = (function () {
  function Rater(db, rateRepository, rateSummaryRepository, usefulRepository) {
    this.db = db;
    this.rateRepository = rateRepository;
    this.rateSummaryRepository = rateSummaryRepository;
    this.usefulRepository = usefulRepository;
    this.getRate = this.getRate.bind(this);
    this.setUseful = this.setUseful.bind(this);
    this.removeUseful = this.removeUseful.bind(this);
    this.rate = this.rate.bind(this);
  }
  Rater.prototype.getRate = function (id, author) {
    return this.rateRepository.load(id, author);
  };
  Rater.prototype.setUseful = function (rateId, userId) {
    return this.usefulRepository.setUseful(rateId, userId);
  };
  Rater.prototype.removeUseful = function (rateId, userId) {
    return this.usefulRepository.removeUseful(rateId, userId);
  };
  Rater.prototype.rate = function (rateReq) {
    return __awaiter(this, void 0, void 0, function () {
      var rate, tx, summary, res, exist, res, history, histories, oldRate, count, err_1;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            rate = { id: rateReq.id, author: rateReq.author, rate: rateReq.rate, time: new Date(), review: rateReq.review };
            return [4, this.db.beginTransaction()];
          case 1:
            tx = _a.sent();
            _a.label = 2;
          case 2:
            _a.trys.push([2, 13, , 15]);
            return [4, this.rateSummaryRepository.exist(rateReq.id, tx)];
          case 3:
            summary = _a.sent();
            if (!!summary) return [3, 6];
            return [4, this.rateRepository.create(rate, true, tx)];
          case 4:
            res = _a.sent();
            return [4, tx.commit()];
          case 5:
            _a.sent();
            return [2, res];
          case 6: return [4, this.rateRepository.load(rateReq.id, rateReq.author, tx)];
          case 7:
            exist = _a.sent();
            if (!!exist) return [3, 10];
            return [4, this.rateRepository.create(rate, false, tx)];
          case 8:
            res = _a.sent();
            return [4, tx.commit()];
          case 9:
            _a.sent();
            return [2, res];
          case 10:
            history = { review: exist.review, rate: exist.rate, time: exist.time };
            if (exist.histories && exist.histories.length > 0) {
              histories = exist.histories;
              histories.push(history);
              exist.histories = histories;
            }
            else {
              exist.histories = [history];
            }
            oldRate = exist.rate;
            exist.rate = rateReq.rate;
            exist.review = rateReq.review;
            exist.time = new Date();
            return [4, this.rateRepository.update(exist, oldRate, tx)];
          case 11:
            count = _a.sent();
            return [4, tx.commit()];
          case 12:
            _a.sent();
            return [2, count];
          case 13:
            err_1 = _a.sent();
            return [4, tx.rollback()];
          case 14:
            _a.sent();
            throw err_1;
          case 15: return [2];
        }
      });
    });
  };
  return Rater;
}());
exports.Rater = Rater;
