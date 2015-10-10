_ = require 'lodash'
Promise = require 'bluebird'
fdb = require('fdb').apiVersion(300)
xxhash = require 'xxhashjs'
RankedSet = require './ranked_set'


# Private Methods
encodeCount = (c) ->
  value = new Buffer(4)
  value.writeInt32LE(c, 0)
  value

decodeCount = (v) ->
  v.readInt32LE(0)

# Return True if there is no other element than item with score.
_no_other = (tr, item_sb, item, score) ->
  r = item_sb.range([score])
  has_element = true
  tr.getRange(r.begin, r.end, { limit: 2 }).toArray()
  .then (kv) ->
    for data in kv
      [s, i] = item_sb.unpack(data.key)
      if i isnt item
        has_element = false
  .then ->
    Promise.resolve(has_element)

# Return scores corresponding to range [start_rank, stop_rank).
_rank_range_to_scores = (tr, subspace, start_rank, stop_rank) ->
  if start_rank < 0 or stop_rank < 0
    throw ('rank must be nonnegative')
  rs_sb = subspace.subspace(['R'])
  RankedSet.getNth(tr, rs_sb, start_rank)
  .then (start_score) ->
    size(tr, subspace)
    .then (total) ->
      if stop_rank > total - 1 # Need to check this
        getMaxScore(tr, subspace)
        .then (stop_score) ->
          console.log start_score, stop_score
          Promise.resolve([start_score, stop_score])
      else
        RankedSet.getNth(tr, rs_sb, stop_rank)
        .then (stop_score) ->
          console.log start_score, stop_score
          Promise.resolve([start_score, stop_score])

# Public Methods

create = (tr, subspace) ->
  RankedSet.create(tr, subspace.subspace(['R']))

# Add item with score, or update its score if item already exists.
insert = (tr, subspace, item, score) ->
  old_score = null
  rs_sb = subspace.subspace(['R'])
  item_sb = subspace.subspace(['I'])
  score_sb = subspace.subspace(['S'])
  counter_sb = subspace.subspace(['C'])
  tr.get(score_sb.get(item))
  .then (s) ->
    if s?
      old_score = decodeCount(s)
      _no_other(tr, item_sb, item, old_score)
      .then ->
        RankedSet.remove(tr, rs_sb, old_score)
      .then ->
        tr.clear(item_sb.get(old_score).get(item))
    else
      Promise.resolve()
  .then ->
    RankedSet.insert(tr, rs_sb, score)
  .then ->
    tr.set(score_sb.get(item), encodeCount(score))
  .then ->
    tr.set(item_sb.get(score).get(item), '')
  .then ->
    tr.add(counter_sb.get('total'), encodeCount(1))
  .then ->
    Promise.resolve(old_score)

# Increase the score of item.
increment = (tr, subspace, item, delta) ->
  rs_sb = subspace.subspace(['R'])
  score_sb = subspace.subspace(['S'])
  item_sb = subspace.subspace(['I'])
  tr.get(score_sb.get(item))
  .then (s) ->
    if s?
      old_score = decodeCount(s)
      score = old_score + delta
      _no_other(tr, item_sb, item, old_score)
      .then ->
        RankedSet.remove(tr, rs_sb, old_score)
      .then ->
        tr.clear(item_sb.get(old_score).get(item))
      .then ->
        RankedSet.insert(tr, rs_sb, score)
      .then ->
        tr.set(score_sb.get(item), encodeCount(score))
      .then ->
        tr.set(item_sb.get(score).get(item), '')
      .then ->
        Promise.resolve(score)
    else
      throw new Error("Item #{item} not found")
      #Promise.resolve() # TODO Create

# Delete item.
remove = (tr, subspace, item) ->
  rs_sb = subspace.subspace(['R'])
  score_sb = subspace.subspace(['S'])
  item_sb = subspace.subspace(['I'])
  counter_sb = subspace.subspace(['C'])
  tr.get(score_sb.get(item))
  .then (s) ->
    if s?
      score  = decodeCount(s)
      _no_other(tr, item_sb, item, score)
      .then (other_element) ->
        unless other_element
          RankedSet.remove(tr, rs_sb, score)
        else
          Promise.resolve()
      .then ->
        tr.clear(item_sb.get(score).get(item))
      .then ->
        tr.clear(score_sb.get(item))
      .then ->
        tr.add(counter_sb.get('total'), encodeCount(-1))
    else
      Promise.resolve()

# Return list of items with given score.
getItems = (tr, subspace, score) ->
  item_sb = subspace.subspace(['I'])
  r = item_sb.range([score])
  tr.getRange(r.begin, r.end).toArray()
  .then (kv) ->
    Promise.resolve (item_sb.get(score).unpack(data.key)[0] for data in kv)

#Get the score associated with item or None if not present.
getScore = (tr, subspace, item) ->
  score_sb = subspace.subspace(['S'])
  tr.get(score_sb.get(item))
  .then (s) ->
    if s?
      decodeCount(s)
    else
      null

# Return list of items with given rank.
getItemsByRank = (tr, subspace, rank) ->
  RankedSet.getNth(tr, subspace.subspace(['R']), rank)
  .then (score) ->
    getItems(tr, subspace, score)

# Return the rank of a given score.
getRankByScore = (tr, subspace, score) ->
  RankedSet.rank(tr, subspace.subspace(['R']), score)

# Return list of items in the range [start_rank, stop_rank).
getRangeByRank = (tr, subspace, start_rank, stop_rank) ->
  _rank_range_to_scores(tr, subspace, start_rank, stop_rank)
  .then ([start_score, stop_score]) ->
    getRangeByScore(tr, subspace, start_score, stop_score)
  # return self.get_range_by_score(tr, start_score, stop_score)

# Return list of items in the range [start_score, stop_score).
# When reverse=True, scores are ordered from high to low.
getRangeByScore = (tr, subspace, start_score, stop_score, reverse = false) ->
  rs_sb = subspace.subspace(['R'])
  item_sb = subspace.subspace(['I'])
  score_sb = subspace.subspace(['S'])
  #RankedSet.getRange(start_score, stop_score)
  #.then ->
  tr.getRange(item_sb.get(start_score), item_sb.get(stop_score), { reverse: reverse }).toArray()
  .then (kv) ->
    set = []
    for data in kv
      key = item_sb.unpack(data.key)
      set.push {
        item: key[1]
        score: key[0]
      }
    Promise.resolve(set)

# Return the rank of a item.
getRank = (tr, subspace, item) ->
  getScore(tr, subspace, item)
  .then (score) ->
    if score?
      getRankByScore(tr, subspace, score)
    else
      Promise.resolve(null)

# Return the maximum rank.
getMaxRank = (tr, subspace) ->
  RankedSet.size(tr, subspace)
  .then (size) ->
    if size?
      size - 1
    else
      null

# Return the maximum score.
getMaxScore = (tr, subspace) ->
  item_sb = subspace.subspace(['I'])
  r = item_sb.range()
  tr.getRange(r.begin, r.end, { limit: 1, reverse: true }).toArray()
  .then (kv) ->
    Promise.resolve(item_sb.unpack(kv[0].key)[0])

# Generate items and their scores.
list = (tr, subspace) ->
  score_sb = subspace.subspace(['S'])
  r = score_sb.range()
  tr.getRange(r.begin, r.end).toArray()
  .then (kv) ->
    Promise.resolve ([score_sb.unpack(data.key)[0], decodeCount(data.value)] for data in kv)

size = (tr, subspace) ->
  counter_sb = subspace.subspace(['C'])
  tr.get(counter_sb.get('total'))
  .then (v) ->
    Promise.resolve(decodeCount(v))

module.exports = {

  create: create

  insert: insert

  increment: increment

  getItems: getItems

  getScore: getScore

  getItemsByRank: getItemsByRank

  getRangeByRank: getRangeByRank

  getRangeByScore: getRangeByScore

  getRank: getRank

  getRankByScore: getRankByScore

  getMaxRank: getMaxRank

  getMaxScore: getMaxScore

  list: list

  size: size

  remove: remove
}
