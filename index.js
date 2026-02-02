// Import the RTMS SDK
import rtms from "@zoom/rtms";
import { spawn } from "child_process";

let clients = new Map();
let pythonProcess = null;

// Start the Python consumer
function startPythonConsumer() {
    if (pythonProcess) return;

    console.log("Starting Python Consumer...");
    // Use -u for unbuffered output
    pythonProcess = spawn("python", ["-u", "cv_consumer.py"], {
        stdio: ['pipe', 'inherit', 'inherit']
    });

    pythonProcess.on('close', (code) => {
        console.log(`Python process exited with code ${code}`);
        pythonProcess = null;
    });
}

startPythonConsumer();

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

    // client.onTranscriptData((data, size, timestamp, metadata) => {
    //   console.log(`[${timestamp}] -- ${metadata.userName}: ${data}`);
    // });

    const video_params = {
        contentType: rtms.VideoContentType.RAW_VIDEO,
        codec: rtms.VideoCodec.H264,
        resolution: rtms.VideoResolution.SD,
        dataOpt: rtms.VideoDataOption.VIDEO_SINGLE_ACTIVE_STREAM,
        fps: 30
    };

    client.setVideoParams(video_params);

    client.onVideoData((data, size, timestamp, metadata) => {
        // console.log(`Received ${size} bytes of video data at ${timestamp} from ${metadata.userName}`);
        if (pythonProcess && pythonProcess.stdin.writable) {
            try {
                pythonProcess.stdin.write(data);
            } catch (err) {
                console.error("Error writing to python stdin:", err);
            }
        }
    });

    // Join the meeting using the webhook payload directly
    client.join(payload);
});

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log("Received SIGINT. Cleaning up...");
    if (pythonProcess) {
        pythonProcess.kill();
    }
    process.exit();
});