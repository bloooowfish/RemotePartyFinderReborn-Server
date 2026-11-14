module.exports = {
  apps: [
    {
      name: "rpf-origin-server",
      cwd: __dirname,
      script: "./server",
      args: [],
      exec_interpreter: "none",
      autorestart: true,
      time: true,
      out_file: "./logs/rpf-origin-server.out.log",
      error_file: "./logs/rpf-origin-server.err.log",
      env: {
        RUST_LOG: process.env.RUST_LOG || "info",
      },
    },
  ],
};
