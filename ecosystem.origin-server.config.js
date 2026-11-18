module.exports = {
  apps: [
    {
      name: "rpf-origin-server",
      cwd: __dirname,
      script: "./server",
      args: [],
      exec_interpreter: "none",
      autorestart: true,
      max_memory_restart: "384M",
      exp_backoff_restart_delay: 1000,
      time: true,
      out_file: "./logs/rpf-origin-server.out.log",
      error_file: "./logs/rpf-origin-server.err.log",
      env: {
        RUST_LOG: process.env.RUST_LOG || "info",
      },
    },
  ],
};
