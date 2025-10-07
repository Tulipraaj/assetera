'use client';

import React from 'react';
import { motion } from 'framer-motion';
import dynamic from 'next/dynamic';

const LoginForm = dynamic(() => import('./login/page'), {
  ssr: false
});

export default function HomePage() {
  return (
    <div className="min-h-screen flex bg-gradient-to-br from-gray-900 to-gray-800">
      {/* Left side - Description */}
      <div className="w-1/2 p-12 flex flex-col justify-center text-white relative overflow-hidden">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
        >
          <motion.h1 
            className="text-5xl font-bold mb-6 bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-teal-400"
            animate={{ scale: [1, 1.02, 1] }}
            transition={{ duration: 2, repeat: Infinity }}
          >
            AssetEra Internal Portal
          </motion.h1>
          <motion.p 
            className="text-xl text-gray-300 mb-8"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.3 }}
          >
            Empowering financial advisors with cutting-edge tools and insights.
          </motion.p>
        </motion.div>

        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.6 }}
          className="space-y-6 text-gray-400"
        >
          <div className="flex items-center space-x-4">
            <motion.div
              whileHover={{ scale: 1.05 }}
              className="bg-gray-800 p-4 rounded-lg"
            >
              <h3 className="text-lg font-semibold text-blue-400 mb-2">Investment Excellence</h3>
              <p>Industry-leading investment strategies and portfolio management tools.</p>
            </motion.div>
          </div>

          <div className="flex items-center space-x-4">
            <motion.div
              whileHover={{ scale: 1.05 }}
              className="bg-gray-800 p-4 rounded-lg"
            >
              <h3 className="text-lg font-semibold text-teal-400 mb-2">Client-Centric Approach</h3>
              <p>Advanced tools for personalized client relationship management.</p>
            </motion.div>
          </div>

          <div className="flex items-center space-x-4">
            <motion.div
              whileHover={{ scale: 1.05 }}
              className="bg-gray-800 p-4 rounded-lg"
            >
              <h3 className="text-lg font-semibold text-purple-400 mb-2">Data-Driven Insights</h3>
              <p>Real-time analytics and market intelligence at your fingertips.</p>
            </motion.div>
          </div>
        </motion.div>

        {/* Animated particles */}
        <motion.div
          className="absolute inset-0 pointer-events-none"
          initial={{ opacity: 0 }}
          animate={{ opacity: 0.1 }}
          transition={{ delay: 1 }}
        >
          {[...Array(5)].map((_, i) => (
            <motion.div
              key={i}
              className="absolute w-32 h-32 bg-blue-500 rounded-full mix-blend-multiply filter blur-xl opacity-20"
              animate={{
                x: [Math.random() * 100, Math.random() * 100],
                y: [Math.random() * 100, Math.random() * 100],
                scale: [1, 1.2, 1],
              }}
              transition={{
                duration: Math.random() * 10 + 10,
                repeat: Infinity,
                repeatType: "reverse",
              }}
              style={{
                left: `${Math.random() * 100}%`,
                top: `${Math.random() * 100}%`,
              }}
            />
          ))}
        </motion.div>
      </div>

      {/* Right side - Login Form */}
      <div className="w-1/2">
        <LoginForm />
      </div>
    </div>
  );
}
