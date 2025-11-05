// Import the RTMS SDK
import rtms from "@zoom/rtms";

let clients = new Map();

// Set up webhook event handler to receive RTMS events from Zoom
rtms.onWebhookEvent(({ event, payload }) => {
  const streamId = payload?.rtms_stream_id;

  if (event == "meeting.rtms_stopped") {
      if (!streamId) {
          console.log(`Received meeting.rtms_stopped event without stream ID`);
          return;
      }

      const client = clients.get(streamId);
      if (!client) {
          console.log(`Received meeting.rtms_stopped event for unknown stream ID: ${streamId}`)
          return 
      }

      client.leave();
      clients.delete(streamId);

      return;
  } else if (event !== "meeting.rtms_started") {
    console.log(`Ignoring unknown event`);
    return;
  }

  // Create a new RTMS client for the stream if it doesn't exist
  const client = new rtms.Client();
  clients.set(streamId, client);


  client.onAudioData((data, size, timestamp, metadata) => {
      console.log(`Received ${size} bytes of audio data at ${timestamp} from ${metadata.userName} -- ${metadata.userId}`);
  });

  client.onSessionUpdate((op, sessionInfo) => {
    console.log(`Session update: ${op} - ${sessionInfo}`);
  });


  // Join the meeting using the webhook payload directly
  client.join(payload);
});