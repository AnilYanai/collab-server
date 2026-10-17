package com.anilyanai.collabclient

import android.Manifest
import android.content.pm.PackageManager
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Color
import android.graphics.Matrix
import android.os.Bundle
import android.view.View
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.annotation.OptIn
import androidx.appcompat.app.AppCompatActivity
import androidx.camera.core.*
import androidx.camera.lifecycle.ProcessCameraProvider
import androidx.core.content.ContextCompat
import com.anilyanai.collabclient.databinding.ActivityMainBinding
import com.anilyanai.collabclient.network.CollabUdpClient
import java.io.ByteArrayOutputStream
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private var udpClient: CollabUdpClient? = null
    private var isConnected = false
    private var isStreamingVideo = false

    private lateinit var cameraExecutor: ExecutorService
    private var cameraProvider: ProcessCameraProvider? = null
    private var lastFrameTime = 0L

    private val hmacSecret = "collab-benchmark-secret-key-256bit-ok"

    private val requestCameraPermissionLauncher =
        registerForActivityResult(ActivityResultContracts.RequestPermission()) { isGranted ->
            if (isGranted) {
                startCamera()
            } else {
                Toast.makeText(this, "Camera permission required for video calls", Toast.LENGTH_SHORT).show()
            }
        }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        cameraExecutor = Executors.newSingleThreadExecutor()

        setupListeners()
    }

    private fun setupListeners() {
        binding.btnConnect.setOnClickListener {
            if (isConnected) {
                disconnect()
            } else {
                connect()
            }
        }

        binding.btnToggleVideo.setOnClickListener {
            if (isStreamingVideo) {
                stopCamera()
            } else {
                checkAndStartCamera()
            }
        }

        binding.btnSend.setOnClickListener {
            val message = binding.etMessage.text.toString().trim()
            if (message.isNotEmpty()) {
                udpClient?.sendBroadcastMessage(message)
                appendLog("[You] $message")
                binding.etMessage.text?.clear()
            }
        }
    }

    private fun connect() {
        val host = binding.etHost.text.toString().trim()
        val port = binding.etPort.text.toString().toIntOrNull() ?: 8080
        val sessionId = binding.etSession.text.toString().trim()
        val userId = binding.etUser.text.toString().trim()

        if (host.isEmpty() || sessionId.isEmpty() || userId.isEmpty()) {
            Toast.makeText(this, "Please fill in all connection fields", Toast.LENGTH_SHORT).show()
            return
        }

        udpClient = CollabUdpClient(
            onJoined = { session ->
                runOnUiThread {
                    isConnected = true
                    binding.tvStatus.text = "Joined: $session"
                    binding.tvStatus.setBackgroundColor(Color.parseColor("#15803D"))
                    binding.tvStatus.setTextColor(Color.WHITE)
                    binding.btnConnect.text = "Leave Session"
                    binding.btnConnect.setBackgroundColor(Color.parseColor("#DC2626"))
                    binding.btnToggleVideo.isEnabled = true
                    binding.btnToggleVideo.setBackgroundColor(Color.parseColor("#0284C7"))
                    appendLog("✅ Joined room '$session'. Video ready!")
                }
            },
            onMessageReceived = { sender, text, latencyMs ->
                runOnUiThread {
                    val latencyBadge = if (latencyMs > 0) " (${latencyMs}ms)" else ""
                    appendLog("[$sender]$latencyBadge: $text")
                }
            },
            onVideoFrameReceived = { _, jpegBytes, latencyMs ->
                runOnUiThread {
                    binding.tvRemotePlaceholder.visibility = View.GONE
                    val bitmap = BitmapFactory.decodeByteArray(jpegBytes, 0, jpegBytes.size)
                    if (bitmap != null) {
                        binding.ivRemoteVideo.setImageBitmap(bitmap)
                        binding.tvVideoStats.text = "Incoming Video: ${jpegBytes.size / 1024} KB | Latency: ${latencyMs}ms"
                    }
                }
            },
            onError = { error ->
                runOnUiThread {
                    appendLog("⚠️ $error")
                }
            },
            onStatusChanged = { status ->
                runOnUiThread {
                    binding.tvStatus.text = status
                }
            }
        )

        udpClient?.connectAndJoin(host, port, sessionId, userId, hmacSecret)
    }

    private fun checkAndStartCamera() {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED) {
            startCamera()
        } else {
            requestCameraPermissionLauncher.launch(Manifest.permission.CAMERA)
        }
    }

    @OptIn(ExperimentalGetImage::class)
    private fun startCamera() {
        val cameraProviderFuture = ProcessCameraProvider.getInstance(this)
        cameraProviderFuture.addListener({
            cameraProvider = cameraProviderFuture.get()

            val preview = Preview.Builder().build().also {
                it.setSurfaceProvider(binding.previewView.surfaceProvider)
            }

            val imageAnalyzer = ImageAnalysis.Builder()
                .setBackpressureStrategy(ImageAnalysis.STRATEGY_KEEP_ONLY_LATEST)
                .setOutputImageFormat(ImageAnalysis.OUTPUT_IMAGE_FORMAT_RGBA_8888)
                .build()
                .also {
                    it.setAnalyzer(cameraExecutor) { imageProxy ->
                        processCameraFrame(imageProxy)
                    }
                }

            val cameraSelector = CameraSelector.DEFAULT_FRONT_CAMERA

            try {
                cameraProvider?.unbindAll()
                cameraProvider?.bindToLifecycle(this, cameraSelector, preview, imageAnalyzer)
                isStreamingVideo = true
                runOnUiThread {
                    binding.btnToggleVideo.text = "Stop Camera"
                    binding.btnToggleVideo.setBackgroundColor(Color.parseColor("#E11D48"))
                    binding.cardLocalPreview.visibility = View.VISIBLE
                    appendLog("📹 Front Camera streaming active (15 FPS).")
                }
            } catch (exc: Exception) {
                appendLog("Camera start failed: ${exc.message}")
            }
        }, ContextCompat.getMainExecutor(this))
    }

    private fun processCameraFrame(imageProxy: ImageProxy) {
        val now = System.currentTimeMillis()
        // Throttle to ~15 FPS to conserve bandwidth and CPU
        if (now - lastFrameTime < 65) {
            imageProxy.close()
            return
        }
        lastFrameTime = now

        try {
            val bitmap = imageProxy.toBitmap()
            val matrix = Matrix().apply {
                postRotate(imageProxy.imageInfo.rotationDegrees.toFloat())
                // Mirror front camera
                postScale(-1f, 1f, bitmap.width / 2f, bitmap.height / 2f)
            }
            val rotated = Bitmap.createBitmap(bitmap, 0, 0, bitmap.width, bitmap.height, matrix, true)
            val scaled = Bitmap.createScaledBitmap(rotated, 320, 240, true)

            val stream = ByteArrayOutputStream()
            scaled.compress(Bitmap.CompressFormat.JPEG, 45, stream)
            val jpegBytes = stream.toByteArray()

            udpClient?.sendVideoFrame(jpegBytes)
        } catch (_: Exception) {
        } finally {
            imageProxy.close()
        }
    }

    private fun stopCamera() {
        cameraProvider?.unbindAll()
        isStreamingVideo = false
        binding.btnToggleVideo.text = "Start Camera"
        binding.btnToggleVideo.setBackgroundColor(Color.parseColor("#0284C7"))
        binding.cardLocalPreview.visibility = View.GONE
        appendLog("⏹️ Camera stopped.")
    }

    private fun disconnect() {
        stopCamera()
        udpClient?.disconnect()
        udpClient = null
        isConnected = false
        binding.tvStatus.text = "Disconnected"
        binding.tvStatus.setBackgroundColor(Color.parseColor("#334155"))
        binding.tvStatus.setTextColor(Color.parseColor("#94A3B8"))
        binding.btnConnect.text = "Join Session"
        binding.btnConnect.setBackgroundColor(Color.parseColor("#0284C7"))
        binding.btnToggleVideo.isEnabled = false
        binding.btnToggleVideo.setBackgroundColor(Color.parseColor("#475569"))
        binding.tvRemotePlaceholder.visibility = View.VISIBLE
        binding.ivRemoteVideo.setImageDrawable(null)
        appendLog("🔌 Disconnected from session.")
    }

    private fun appendLog(line: String) {
        val currentText = binding.tvChatLog.text.toString()
        binding.tvChatLog.text = "$currentText\n$line"
        binding.scrollView.post {
            binding.scrollView.fullScroll(View.FOCUS_DOWN)
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        cameraExecutor.shutdown()
        disconnect()
    }
}
