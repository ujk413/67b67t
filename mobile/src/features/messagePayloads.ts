/** Call only AFTER existing DM / sender-key decryption. The encoded text must
 * go through onSend / repository outbox so encryption and recipient binding stay intact. */
export {PAYLOAD_PREFIX,MAX_LIVE_LOCATION_MS,parseMessagePayload,encodeMessagePayload,
 validateMessagePayload,expressiveId,secureDice,quizCommitment,reducePoll,reduceLocation,
 isExpressiveControl,expressiveSummary,searchEmoji,EMOJI_CATALOG,EMOJI_CATEGORIES,isSecretRoom,buildChatExport}
 from '../../../web/expressive-payloads.js';
export type {MessagePayload,StickerPayload,ContactPayload,DicePayload,PollPayload,
 PollVotePayload,PollClosePayload,LocationPayload,LocationUpdatePayload,LocationStopPayload,
 PayloadMessage,PollState,LocationState} from '../../../web/expressive-payloads.js';
