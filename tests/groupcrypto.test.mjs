import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const GC = createRequire(import.meta.url)('../web/groupcrypto.js');
const ROOM = 'room-1';
const ALICE = 'alice';

async function sender() {
  return GC.createSenderKey();
}

test('round-trip with a member', async () => {
  let s = await sender();
  const member = GC.receiverStateFromDistribution(GC.distributionFor(s));
  const r = await GC.encryptWithState(s, ROOM, ALICE, 'привет');
  s = r.state;
  const d = await GC.decryptWithState(member, ROOM, ALICE, r.envelope);
  assert.equal(d.plaintext, 'привет');
});

test('sender can re-read its own history from the genesis chain', async () => {
  let s = await sender();
  const sent = [];
  for (const t of ['one', 'two', 'three']) {
    const r = await GC.encryptWithState(s, ROOM, ALICE, t);
    s = r.state;
    sent.push(r.envelope);
  }
  let own = GC.ownReceiverState(s);
  for (let i = 0; i < sent.length; i++) {
    const d = await GC.decryptWithState(own, ROOM, ALICE, sent[i]);
    own = d.state;
    assert.equal(d.plaintext, ['one', 'two', 'three'][i]);
  }
});

test('out-of-order and skipped messages decrypt; replay is refused', async () => {
  let s = await sender();
  const envs = [];
  for (let i = 0; i < 4; i++) {
    const r = await GC.encryptWithState(s, ROOM, ALICE, `m${i}`);
    s = r.state;
    envs.push(r.envelope);
  }
  let member = GC.receiverStateFromDistribution({ v: 1, kid: envs[0].kid, chain: GC.ownReceiverState(s).chain, n: 0, sig_pub: s.sign.pubRaw });

  let d = await GC.decryptWithState(member, ROOM, ALICE, envs[2]);
  assert.equal(d.plaintext, 'm2');
  member = d.state;
  d = await GC.decryptWithState(member, ROOM, ALICE, envs[0]);
  assert.equal(d.plaintext, 'm0');
  member = d.state;
  await assert.rejects(GC.decryptWithState(member, ROOM, ALICE, envs[0]), /message_key_consumed/);
});

test('tampering is rejected: ciphertext, room, sender, iteration', async () => {
  let s = await sender();
  const member = GC.receiverStateFromDistribution(GC.distributionFor(s));
  const r = await GC.encryptWithState(s, ROOM, ALICE, 'secret');
  const e = r.envelope;
  const flip = (b64) => { const u = Uint8Array.from(atob(b64), c => c.charCodeAt(0)); u[0] ^= 1; return btoa(String.fromCharCode(...u)); };
  await assert.rejects(GC.decryptWithState(member, ROOM, ALICE, { ...e, ct: flip(e.ct) }), /bad_signature|OperationError/);
  await assert.rejects(GC.decryptWithState(member, 'room-2', ALICE, e), /bad_signature|OperationError/);
  await assert.rejects(GC.decryptWithState(member, ROOM, 'mallory', e), /bad_signature|OperationError/);
  await assert.rejects(GC.decryptWithState(member, ROOM, ALICE, { ...e, n: 1 }), /bad_signature|OperationError/);
  await assert.rejects(GC.decryptWithState(member, ROOM, ALICE, { ...e, sig: flip(e.sig) }), /bad_signature|OperationError/);
});

test('a forged far-future envelope does not advance the stored chain', async () => {
  let s = await sender();
  const member = GC.receiverStateFromDistribution(GC.distributionFor(s));
  const before = JSON.stringify(member);
  const r = await GC.encryptWithState(s, ROOM, ALICE, 'x');
  await assert.rejects(GC.decryptWithState(member, ROOM, ALICE, { ...r.envelope, n: 900 }));
  assert.equal(JSON.stringify(member), before);
});

test('delta cap refuses huge jumps', async () => {
  const s = await sender();
  const member = GC.receiverStateFromDistribution(GC.distributionFor(s));
  const r = await GC.encryptWithState(s, ROOM, ALICE, 'x');
  await assert.rejects(GC.decryptWithState(member, ROOM, ALICE, { ...r.envelope, n: GC.MAX_DELTA + 10 }), /delta_too_large/);
});

test('a late joiner gets the current iteration and cannot read earlier messages', async () => {
  let s = await sender();
  const early = [];
  for (let i = 0; i < 3; i++) {
    const r = await GC.encryptWithState(s, ROOM, ALICE, `early${i}`);
    s = r.state;
    early.push(r.envelope);
  }
  // Member joins now: distribution carries the current chain and iteration.
  const late = GC.receiverStateFromDistribution(GC.distributionFor(s));
  await assert.rejects(GC.decryptWithState(late, ROOM, ALICE, early[0]), /delta_too_large|message_key_consumed|bad_signature/);
  const r = await GC.encryptWithState(s, ROOM, ALICE, 'late');
  const d = await GC.decryptWithState(late, ROOM, ALICE, r.envelope);
  assert.equal(d.plaintext, 'late');
});

test('rotation: a removed member cannot read the new key', async () => {
  let s = await sender();
  const removed = GC.receiverStateFromDistribution(GC.distributionFor(s));
  const rotated = await GC.createSenderKey();
  const r = await GC.encryptWithState(rotated, ROOM, ALICE, 'after removal');
  await assert.rejects(GC.decryptWithState(removed, ROOM, ALICE, r.envelope), /unknown_key/);
});

test('malformed distribution is rejected', () => {
  assert.throws(() => GC.receiverStateFromDistribution({ v: 2 }), /bad_distribution/);
  assert.throws(() => GC.receiverStateFromDistribution({ v: 1, kid: 'k', chain: 'c', n: -1, sig_pub: 'p' }), /bad_distribution/);
});

test('planSend: a departed recipient forces rotation, and their old key cannot read what follows', async () => {
  const S = 'sender';
  let rec = await sender();
  rec.distributedTo = ['A', 'C'];
  const rC0 = GC.receiverStateFromDistribution(GC.distributionFor(rec));

  // C is removed; the sender sends again.
  const plan = GC.planSend(rec, [S, 'A'], S);
  assert.equal(plan.rotate, true);
  assert.deepEqual(plan.pending, ['A']);
  const fresh = await GC.createSenderKey();
  fresh.distributedTo = ['A'];
  const rA = GC.receiverStateFromDistribution(GC.distributionFor(fresh));
  const r = await GC.encryptWithState(fresh, ROOM, S, 'after removal');

  assert.equal((await GC.decryptWithState(rA, ROOM, S, r.envelope)).plaintext, 'after removal');
  await assert.rejects(GC.decryptWithState(rC0, ROOM, S, r.envelope), /unknown_key/);
});

test('planSend: no removal means no rotation; only new members get the current chain', async () => {
  const S = 'sender';
  let rec = await sender();
  rec.distributedTo = ['A'];
  let plan = GC.planSend(rec, [S, 'A'], S);
  assert.equal(plan.rotate, false);
  assert.deepEqual(plan.pending, []);

  plan = GC.planSend(rec, [S, 'A', 'B'], S);
  assert.equal(plan.rotate, false);
  assert.deepEqual(plan.pending, ['B']);
});

test('planSend without a stored key always rotates', () => {
  const plan = GC.planSend(null, ['S', 'A'], 'S');
  assert.equal(plan.rotate, true);
  assert.deepEqual(plan.pending, ['A']);
});

test('a re-added member gets the current chain and cannot read messages sent before re-adding', async () => {
  const S = 'sender';
  let rec = await sender();
  rec.distributedTo = ['A'];
  const before = [];
  for (const t of ['b1', 'b2']) {
    const r = await GC.encryptWithState(rec, ROOM, S, t);
    rec = r.state;
    before.push(r.envelope);
  }
  // C is re-added now: distribute the current chain only.
  const dist = GC.receiverStateFromDistribution(GC.distributionFor(rec));
  const r = await GC.encryptWithState(rec, ROOM, S, 'after re-add');
  assert.equal((await GC.decryptWithState(dist, ROOM, S, r.envelope)).plaintext, 'after re-add');
  await assert.rejects(GC.decryptWithState(dist, ROOM, S, before[0]), /message_key_consumed|delta_too_large|bad_signature/);
});
