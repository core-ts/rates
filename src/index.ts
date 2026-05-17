export type DataType =
  | "ObjectId"
  | "date"
  | "datetime"
  | "time"
  | "boolean"
  | "number"
  | "integer"
  | "string"
  | "text"
  | "object"
  | "array"
  | "binary"
  | "primitives"
  | "booleans"
  | "numbers"
  | "integers"
  | "strings"
  | "dates"
  | "datetimes"
  | "times"
export type Operator = "=" | "like" | "!=" | "<>" | ">" | ">=" | "<" | "<="

export interface Attribute {
  name?: string
  column?: string
  type?: DataType
  default?: string | number | Date | boolean
  key?: boolean
  q?: boolean
  noinsert?: boolean
  noupdate?: boolean
  nopatch?: boolean
  version?: boolean
  ignored?: boolean
  true?: string | number
  false?: string | number
  createdAt?: boolean
  updatedAt?: boolean
}
export interface Attributes {
  [key: string]: Attribute
}

export interface Executor {
  driver: string
  param(i: number): string
  execute(sql: string, args?: any[], ctx?: any): Promise<number>
  executeBatch(statements: Statement[], firstSuccess?: boolean, ctx?: any): Promise<number>
  query<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T[]>
}
export interface Transaction extends Executor {
  commit(): Promise<void>
  rollback(): Promise<void>
}
export interface DB extends Executor {
  beginTransaction(): Promise<Transaction>
}
export interface StringMap {
  [key: string]: string
}
export interface Statement {
  query: string
  params?: any[]
}

export function buildMap(attrs: Attributes): StringMap {
  const mp: StringMap = {}
  const ks = Object.keys(attrs)
  for (const k of ks) {
    const attr = attrs[k]
    attr.name = k
    const field = attr.column ? attr.column : k
    const s = field.toLowerCase()
    if (s !== k) {
      mp[s] = k
    }
  }
  return mp
}
export class SqlRateRepository<R> {
  constructor(protected db: DB, protected table: string, protected attributes: Attributes, protected max: number, protected infoTable: string,
    protected buildToInsert: (obj: R, table: string, attrs: Attributes, buildParam: (i: number) => string) => Statement,
    protected buildToUpdate: (obj: R, table: string, attrs: Attributes, buildParam: (i: number) => string) => Statement,
    protected generateId: () => string, protected rateIdField: string, rateField?: string, count?: string, score?: string, authorCol?: string, id?: string, idField?: string, idCol?: string, rateCol?: string) {
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
    } else {
      const c = attributes[this.idField];
      if (c) {
        this.idCol = (c.column && c.column.length > 0 ? c.column : this.idField);
      } else {
        this.idCol = this.idField;
      }
    }
    if (rateCol && rateCol.length > 0) {
      this.rate = rateCol;
    } else {
      const c = attributes[this.rateField];
      if (c) {
        this.rate = (c.column && c.column.length > 0 ? c.column : this.rateField);
      } else {
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
  map?: StringMap;
  count: string;
  score: string;
  id: string;
  rate: string;
  idField: string;
  rateField: string;
  idCol: string;
  authorCol: string;
  load(id: string, author: string, tx?: Transaction): Promise<R | null> {
    const db = tx ? tx : this.db
    return db.query<R>(`select * from ${this.table} where ${this.idCol} = ${this.db.param(1)} and ${this.authorCol} = ${this.db.param(2)}`, [id, author], this.map).then(rates => {
      return rates && rates.length > 0 ? rates[0] : null;
    });
  }
  create(rate: R, newInfo?: boolean, tx?: Transaction): Promise<number> {
    (rate as any)[this.rateIdField] = this.generateId()
    const stmt = this.buildToInsert(rate, this.table, this.attributes, this.db.param);
    if (stmt.query) {
      const obj: any = rate;
      const rateNum: number = obj[this.rateField];
      const id: string = obj[this.idField];
      const db = tx ? tx : this.db
      if (newInfo) {
        const query = this.insertInfo(rateNum);
        const s2: Statement = { query, params: [id] };
        return db.executeBatch([s2, stmt], true);
      } else {
        const query = this.updateNewInfo(rateNum);
        const s2: Statement = { query, params: [id] };
        return db.executeBatch([s2, stmt], true);
      }
    } else {
      return Promise.resolve(-1);
    }
  }
  protected insertInfo(r: number): string {
    const rateCols: string[] = [];
    const ps: string[] = [];
    for (let i = 1; i <= this.max; i++) {
      rateCols.push(`${this.rate}${i}`);
      if (i === r) {
        ps.push('' + 1);
      } else {
        ps.push('0');
      }
    }
    const query = `
      insert into ${this.infoTable} (${this.id}, ${this.rate}, ${this.count}, ${this.score}, ${rateCols.join(',')})
      values (${this.db.param(1)}, ${r}, 1, ${r}, ${ps.join(',')})`;
    return query;
  }
  update(rate: R, oldRate: number, tx?: Transaction): Promise<number> {
    const stmt = this.buildToUpdate(rate, this.table, this.attributes, this.db.param);
    if (stmt.query) {
      const obj: any = rate;
      const rateNum: number = obj[this.rateField];
      const id: string = obj[this.idField];
      const query = this.updateOldInfo(rateNum, oldRate);
      const s2: Statement = { query, params: [id] };
      const db = tx ? tx : this.db
      return db.executeBatch([s2, stmt], true);
    } else {
      return Promise.resolve(-1);
    }
  }
  protected updateNewInfo(r: number): string {
    const query = `
      update ${this.infoTable} set ${this.rate} = (${this.score} + ${r})/(${this.count} + 1), ${this.count} = ${this.count} + 1, ${this.score} = ${this.score} + ${r}, ${this.rate}${r} = ${this.rate}${r} + 1
      where ${this.id} = ${this.db.param(1)}`;
    return query;
  }
  protected updateOldInfo(newRate: number, oldRate: number): string {
    if (newRate === oldRate) {
      return '';
    }
    const delta = newRate - oldRate;
    const query = `
      update ${this.infoTable} set ${this.rate} = (${this.score} + (${delta}))/${this.count}, ${this.score} = ${this.score} + (${delta}), ${this.rate}${newRate} = ${this.rate}${newRate} + 1, ${this.rate}${oldRate} = ${this.rate}${oldRate} - 1
      where ${this.id} = ${this.db.param(1)}`;
    return query;
  }
}

export interface SubmittedRate {
  id: string;
  author: string;
  rate: number;
  review: string;
}
export interface Rate {
  id: string;
  author: string;
  rate: number;
  time: Date;
  review: string;
  histories?: History[];
}

export interface History {
  rate: number;
  time: Date;
  review: string;
}

export interface RateRepository {
  create(rate: Rate, newInfo?: boolean, tx?: Transaction): Promise<number>
  update(rate: Rate, oldRate: number, tx?: Transaction): Promise<number>
  load(id: string, author: string, tx?: Transaction): Promise<Rate | null>
}
export interface RateSummaryRepository {
  exist(id: string, tx?: Transaction): Promise<boolean>
}
export interface RateService {
  getRate(id: string, author: string): Promise<Rate | null>
  setUseful(rateId: string, userId: string): Promise<number>
  removeUseful(rateId: string, userId: string): Promise<number>
  rate(rateReq: SubmittedRate): Promise<number>
}
export interface UsefulRepository {
  setUseful(rateId: string, userId: string): Promise<number>
  removeUseful(rateId: string, userId: string): Promise<number>
}
// tslint:disable-next-line:max-classes-per-file
export class Rater implements RateService {
  constructor(protected db: DB, protected rateRepository: RateRepository, protected rateSummaryRepository: RateSummaryRepository, protected usefulRepository: UsefulRepository) {
    this.getRate = this.getRate.bind(this)
    this.setUseful = this.setUseful.bind(this)
    this.removeUseful = this.removeUseful.bind(this)
    this.rate = this.rate.bind(this)
  }
  getRate(id: string, author: string): Promise<Rate | null> {
    return this.rateRepository.load(id, author)
  }
  setUseful(rateId: string, userId: string): Promise<number> {
    return this.usefulRepository.setUseful(rateId, userId)
  }
  removeUseful(rateId: string, userId: string): Promise<number> {
    return this.usefulRepository.removeUseful(rateId, userId)
  }
  async rate(rateReq: SubmittedRate): Promise<number> {
    const rate: Rate = {id: rateReq.id, author: rateReq.author, rate: rateReq.rate, time: new Date(), review: rateReq.review}
    const tx = await this.db.beginTransaction()
    try {
      const summary = await this.rateSummaryRepository.exist(rateReq.id, tx)
      if (!summary) {
        const res = await this.rateRepository.create(rate, true, tx)
        await tx.commit()
        return res;
      }
      const exist = await this.rateRepository.load(rateReq.id, rateReq.author, tx)
      if (!exist) {
        const res = await this.rateRepository.create(rate, false, tx)
        await tx.commit()
        return res;
      }
      const history: History = { review: exist.review, rate: exist.rate, time: exist.time }
      if (exist.histories && exist.histories.length > 0) {
        const histories = exist.histories;
        histories.push(history);
        exist.histories = histories;
      } else {
        exist.histories = [history];
      }
      const oldRate = exist.rate
      exist.rate = rateReq.rate
      exist.review = rateReq.review
      exist.time = new Date()
      const count = await this.rateRepository.update(exist, oldRate, tx)
      await tx.commit()
      return count
    } catch (err) {
      await tx.rollback()
      throw err
    }
  }
}
