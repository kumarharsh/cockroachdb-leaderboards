Promise = require 'bluebird'
_ = require 'lodash'
fdb = require('fdb').apiVersion(300)
ScoredSet = require '../src/scored_set'

describe 'The Scored Set data structure', ->

  beforeEach (next) ->

    @encodeValue = (value) ->
      data = new Buffer(4)
      data.writeUInt32LE(value, 0)
      data

    @decodeValue = (buffer) ->
      buffer.readUInt32LE(0)

    @transaction (tr) =>
      tr.clearRange(@subspace.pack([]), @subspace.pack([0xff]))
    .then ->
      next()
    .done()

  describe 'the insert function', ->

    it 'adds the value in the set', (next) ->
      subspace = @subspace.subspace(['rs'])
      @transaction (tr) ->
        ScoredSet.create(tr, subspace)
        .then =>
          Promise.reduce [1..100], (total, index) ->
            score = parseInt(Math.random() * 100) * (if Math.random() > 0.5 then 1 else -1)
            ScoredSet.insert(tr, subspace, "player"+index, score)
          , 0
      .then =>
        range = subspace.range()
        counts = {}
        @transaction (tr) =>
          tr.getRange(range.begin, range.end).toArray()
          .then (results) =>
            console.log subspace.unpack(res.key) for res in results
          .then ->
            ScoredSet.getRankByScore(tr, subspace, 11)
          .then (rank) ->
            console.log rank
            ScoredSet.getItems(tr, subspace, 11)
          .then (items) ->
            console.log items
            ScoredSet.getScore(tr, subspace, "player"+11)
          .then (score) ->
            console.log score
            ScoredSet.getItemsByRank(tr, subspace, 4)
          .then (items) ->
            console.log items
            ScoredSet.list(tr, subspace)
          .then (all) ->
            console.log all
            # for pair in results
            #   [level, key] = subspace.unpack(pair.key)
            #   # Every key must be an inserted item
            #   expect(items.indexOf(key) >= 0).to.equal.true
            #   counts[level] ?= 0
            #   counts[level] += @decodeValue(pair.value)
        # .then =>
        #   # The total count on each level must equal the total number of items
        #   for level, total of counts
        #     total.should.equal items.length
      .then ->
        next()
      .done()
      return

  describe 'ScoredSet', ->

    beforeEach ->
      subspace = @subspace.subspace(['rs'])
      players = [23, 34, 55, 12, 3, 3, 34, 12, 424,53, 64]
      @transaction (tr) =>
        ScoredSet.create(tr, subspace)
        .then =>
          current = Promise.cast()
          _.forEach players, (score, index) ->
            current = current.then ->
              ScoredSet.insert(tr, subspace, "player#{index+1}", score)
          current

    it 'creates a scored set', ->
      subspace = @subspace.subspace(['rs'])
      range = subspace.range()
      @transaction (tr) ->
        tr.getRange(range.begin, range.end).toArray()
        .then (results) =>
          @assertKVPairsAreEqual(subspace, results, [
            ['R',0,''], @encodeValue(0)
            ['R',1,''], @encodeValue(0)
            ['R',2,''], @encodeValue(0)
            ['R',3,''], @encodeValue(0)
            ['R',4,''], @encodeValue(0)
            ['R',5,''], @encodeValue(0)
          ])
          Promise.resolve()
      .then ->
        next()
      .done()
      return

    it 'should increment a player score', ->
      subspace = @subspace.subspace(['rs'])
      @transaction (tr) =>
        ScoredSet.getRank(tr, subspace, 'player5')
        .then (rank) =>
          rank.should.equal 0
          ScoredSet.getScore(tr, subspace, 'player5')
        .then (score) =>
          score.should.equal 3
          ScoredSet.increment(tr, subspace, 'player5', 25)
        .then (inc) =>
          inc.should.equal 28
          ScoredSet.getRank(tr, subspace, 'player5')
        .then (rank) =>
          rank.should.equal 2
          ScoredSet.getScore(tr, subspace, 'player5')
        .then (score) =>
          score.should.equal 28
          ScoredSet.list(tr, subspace)
        .then (list) =>
          console.log list
          ScoredSet.size(tr, subspace)
        .then (total) =>
          console.log total

    it 'should read a players rank and score', ->
      subspace = @subspace.subspace(['rs'])
      players = [23, 34, 55, 12, 3, 3, 34]
      @transaction (tr) =>
        ScoredSet.getRank(tr, subspace, 'player3')
        .then (rank) =>
          rank.should.equal 4
          ScoredSet.getRank(tr, subspace, 'player1')
        .then (rank) =>
          rank.should.equal 2
          ScoredSet.getScore(tr, subspace, 'player4')
        .then (score) =>
          Promise.all(ScoredSet.getScore(tr, subspace, "player#{i}") for i in [1..7])
        .then (scores) =>
          scores.should.deep.equal players

    it 'should remove a player', ->
      subspace = @subspace.subspace(['rs'])
      @transaction (tr) =>
        ScoredSet.size(tr, subspace)
        .then (size) ->
          size.should.equal 7
          ScoredSet.getRank(tr, subspace, 'player3')
        .then (rank) =>
          rank.should.equal 4
          ScoredSet.getScore(tr, subspace, 'player3')
        .then (score) =>
          score.should.equal 55
        .then =>
          ScoredSet.remove(tr, subspace, 'player3')
        .then =>
          ScoredSet.getRank(tr, subspace, 'player3')
        .then (rank) =>
          (rank is null).should.equal true
          ScoredSet.getScore(tr, subspace, 'player3')
        .then (score) =>
          (score is null).should.equal true
          ScoredSet.size(tr, subspace)
        .then (size) ->
          size.should.equal 6

    it 'should get all players with a particular score', ->
      subspace = @subspace.subspace(['rs'])
      @transaction (tr) =>
        ScoredSet.getItems(tr, subspace, 3)
        .then (items) =>
          items.should.deep.equal ['player5', 'player6']
        .then =>
          ScoredSet.getRank(tr, subspace, 'player5')
        .then (rank) =>
          rank.should.equal 0
          ScoredSet.getRank(tr, subspace, 'player6')
        .then (rank) =>
          rank.should.equal 0
          ScoredSet.getItems(tr, subspace, 34)
        .then (items) =>
          items.should.deep.equal ['player2', 'player7']
          ScoredSet.getItems(tr, subspace, 12)
        .then (items) =>
          items.should.deep.equal ['player4']

    it 'the list function', ->
      subspace = @subspace.subspace(['rs'])
      @transaction (tr) =>
        ScoredSet.list(tr, subspace)
        .then (list) =>
          console.log list
          ScoredSet.getMaxScore(tr, subspace)
        .then (max) =>
          #max.should.equal 55
          ScoredSet.getRangeByRank(tr, subspace, 0, 10)
        .then (range) =>
          console.log range

  describe 'Benchmark', ->

    it.only 'should do 1M inserts', (next) ->
      @timeout 200000
      subspace = @subspace.subspace(['rs'])
      now = new Date()
      @transaction (tr) =>
        ScoredSet.create(tr, subspace)
      .then =>
        current = Promise.cast()
        _.forEach (i for i in [1..10]), (k) =>
          _.forEach (i for i in [1..1000]), (index) =>
            current = current.then =>
              @transaction (trr) =>
                ScoredSet.insert(trr, subspace, "player#{index*k+1}", index*k)
        current
      .then =>
        console.log "Time Taken: #{(new Date().getTime() - now.getTime())/1000} seconds"
        next()
