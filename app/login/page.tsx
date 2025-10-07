"use client";
import { useState } from "react";
import { supabase } from "@/lib/supabaseClient";
import { useRouter } from "next/navigation";
import { useCallback } from "react";
import type { User } from '@supabase/supabase-js';

const ROLES = [
  { value: "advisor", label: "Financial Advisor" },
  { value: "analyst", label: "Business Analyst" },
  { value: "asset_manager", label: "Asset Manager" },
];

export default function LoginPage() {
  const [isRegister, setIsRegister] = useState(false);
  const [email, setEmail] = useState("");
  const [name, setName] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState(ROLES[0].value);
  const [error, setError] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [validationErrors, setValidationErrors] = useState<{ [key: string]: string }>({});
  const [registrationSuccess, setRegistrationSuccess] = useState(false);
  const router = useRouter();

  const validateInputs = useCallback(() => {
    const errors: { [key: string]: string } = {};
    
    if (!email) {
      errors.email = "Email is required";
    } else if (!/\S+@\S+\.\S+/.test(email)) {
      errors.email = "Please enter a valid email address";
    }
    
    if (!password) {
      errors.password = "Password is required";
    } else if (password.length < 6) {
      errors.password = "Password must be at least 6 characters";
    }
    
    if (isRegister && !name) {
      errors.name = "Name is required";
    }

    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  }, [email, password, name, isRegister]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setIsLoading(true);
    setRegistrationSuccess(false);

    try {
      if (!validateInputs()) {
        setIsLoading(false);
        return;
      }

      if (isRegister) {
        // Register with email confirmation
        const { data: signUpData, error: signUpError } = await supabase.auth.signUp({
          email,
          password,
          options: {
            data: {
              name,
              role,
              created_at: new Date().toISOString()
            },
            emailRedirectTo: `${window.location.origin}/login`
          }
        });
        
        if (signUpError) {
          if (signUpError.message.includes("User already registered")) {
            throw new Error("This email is already registered. Please login instead.");
          }
          throw new Error(signUpError.message);
        }

        // Check if the signup was successful
        if (!signUpData.user) {
          throw new Error("Registration failed. Please try again.");
        }

        // Create advisor profile right after registration
        const { error: profileError } = await supabase
          .from('advisors')
          .insert([{
            id: signUpData.user.id,
            name,
            email,
            role,
            created_at: new Date().toISOString()
          }]);

        if (profileError) {
          console.error('Failed to create advisor profile:', profileError);
          // Log error but don't throw since the user is already registered
        }

        // Show success message with email verification instructions
        setRegistrationSuccess(true);
        setError("");
        setEmail("");
        setPassword("");
        return;
      } 

      // Login
      const { data: signInData, error: signInError } = await supabase.auth.signInWithPassword({
        email,
        password,
      });
      
      if (signInError) {
        if (signInError.message.includes("Invalid login credentials")) {
          throw new Error("Invalid email or password. Please try again.");
        }
        throw new Error(signInError.message);
      }

      // Check if we have user data
      if (!signInData?.user) {
        throw new Error("Login successful but no user data received");
      }

      const userId = signInData.user.id;
      
      try {
        // Check if advisor profile exists
        const { data: advisor } = await supabase
          .from("advisors")
          .select("id")
          .eq("id", userId)
          .single();

        if (!advisor) {
          // Create advisor profile
          const { error: insertError } = await supabase
            .from("advisors")
            .insert([{
              id: userId,
              name: signInData.user.user_metadata.name || name || email.split('@')[0],
              email: signInData.user.email || email,
              role: signInData.user.user_metadata.role || role,
              created_at: new Date().toISOString()
            }]);

          if (insertError) {
            throw new Error("Failed to create advisor profile");
          }
        }

        // Get role for routing
        const { data: user } = await supabase
          .from("advisors")
          .select("role")
          .eq("id", userId)
          .single();

        // Route based on role
        const userRole = user?.role || 'advisor';
        switch (userRole) {
          case "advisor":
            router.push("/financial-advisor");
            break;
          case "analyst":
            router.push("/analyst-dashboard");
            break;
          case "asset_manager":
            router.push("/asset-manager-dashboard");
            break;
          default:
            router.push("/");
        }
      } catch (profileError: any) {
        throw new Error(profileError.message || "Failed to setup user profile");
      }
    } catch (err: any) {
      setError(err.message || "An error occurred during authentication");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-blue-900 via-indigo-900 to-purple-900">
      <form onSubmit={handleSubmit} className="bg-white p-8 rounded-xl shadow-2xl w-full max-w-md transform transition-all hover:scale-[1.01]">
        <h2 className="text-3xl font-bold mb-6 text-center text-gray-800 bg-gradient-to-r from-blue-600 to-purple-600 bg-clip-text text-transparent">
          {isRegister ? "Create Account" : "Welcome Back"}
        </h2>
        {error && (
          <div className="mb-6 p-4 bg-red-50 border-l-4 border-red-500 rounded-r-lg">
            <div className="flex">
              <div className="flex-shrink-0">
                <svg className="h-5 w-5 text-red-400" viewBox="0 0 20 20" fill="currentColor">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
                </svg>
              </div>
              <div className="ml-3">
                <p className="text-sm text-red-700">{error}</p>
              </div>
            </div>
          </div>
        )}
        {registrationSuccess && !error && (
          <div className="mb-6 p-4 bg-green-50 border-l-4 border-green-500 rounded-r-lg">
            <div className="flex">
              <div className="flex-shrink-0">
                <svg className="h-5 w-5 text-green-400" viewBox="0 0 20 20" fill="currentColor">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                </svg>
              </div>
              <div className="ml-3">
                <p className="text-sm text-green-700">
                  Registration successful! We've sent you a confirmation email. Please check your inbox and click the verification link to complete your registration.
                </p>
                <p className="mt-2 text-sm text-green-700">
                  After verifying your email, you can return here to log in.
                </p>
              </div>
            </div>
          </div>
        )}
        {isRegister && (
          <div className="mb-6">
            <div className="relative">
              <input
                type="text"
                placeholder="Full Name"
                className={`w-full px-4 py-3 border-2 rounded-lg transition-all bg-gray-50 focus:bg-white ${
                  validationErrors.name ? 'border-red-500' : 'border-gray-200 focus:border-blue-500'
                }`}
                value={name}
                onChange={e => setName(e.target.value)}
              />
              <div className="absolute inset-y-0 left-3 flex items-center pointer-events-none">
                <svg className="h-5 w-5 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
                </svg>
              </div>
            </div>
            {validationErrors.name && (
              <p className="mt-2 text-red-600 text-sm">{validationErrors.name}</p>
            )}
          </div>
        )}
        <div className="mb-6">
          <div className="relative">
            <input
              type="email"
              placeholder="Email Address"
              className={`w-full pl-10 pr-4 py-3 border-2 rounded-lg transition-all bg-gray-50 focus:bg-white ${
                validationErrors.email ? 'border-red-500' : 'border-gray-200 focus:border-blue-500'
              }`}
              value={email}
              onChange={e => setEmail(e.target.value)}
            />
            <div className="absolute inset-y-0 left-3 flex items-center pointer-events-none">
              <svg className="h-5 w-5 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
              </svg>
            </div>
          </div>
          {validationErrors.email && (
            <p className="mt-2 text-red-600 text-sm">{validationErrors.email}</p>
          )}
        </div>
        <div className="mb-6">
          <div className="relative">
            <input
              type="password"
              placeholder="Password"
              className={`w-full pl-10 pr-4 py-3 border-2 rounded-lg transition-all bg-gray-50 focus:bg-white ${
                validationErrors.password ? 'border-red-500' : 'border-gray-200 focus:border-blue-500'
              }`}
              value={password}
              onChange={e => setPassword(e.target.value)}
            />
            <div className="absolute inset-y-0 left-3 flex items-center pointer-events-none">
              <svg className="h-5 w-5 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
              </svg>
            </div>
          </div>
          {validationErrors.password && (
            <p className="mt-2 text-red-600 text-sm">{validationErrors.password}</p>
          )}
          {isRegister && (
            <p className="mt-2 text-gray-600 text-sm">
              Password must be at least 6 characters long
            </p>
          )}
        </div>
        {isRegister && (
          <div className="mb-6">
            <select
              className="w-full pl-10 pr-4 py-3 border-2 border-gray-200 rounded-lg bg-gray-50 focus:border-blue-500 focus:bg-white"
              value={role}
              onChange={e => setRole(e.target.value)}
            >
              {ROLES.map(r => (
                <option key={r.value} value={r.value}>{r.label}</option>
              ))}
            </select>
          </div>
        )}
        <button
          type="submit"
          disabled={isLoading}
          className={`w-full py-3 px-4 rounded-lg text-white font-medium transition-all transform hover:translate-y-[-1px] ${
            isLoading 
              ? 'bg-blue-400 cursor-not-allowed'
              : 'bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 active:from-blue-800 active:to-purple-800 shadow-lg hover:shadow-xl'
          }`}
        >
          {isLoading ? (
            <span className="flex items-center justify-center">
              <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
              </svg>
              Processing...
            </span>
          ) : (
            isRegister ? "Create Account" : "Sign In"
          )}
        </button>
        <div className="mt-6 text-center">
          <button
            type="button"
            className="text-gray-600 hover:text-blue-600 transition-all text-sm font-medium"
            onClick={() => {
              setIsRegister(r => !r);
              setError("");
              setValidationErrors({});
              setRegistrationSuccess(false);
              setEmail("");
              setPassword("");
              setName("");
            }}
          >
            {isRegister ? "Already have an account? Sign in" : "Don't have an account? Create one"}
          </button>
        </div>
      </form>
    </div>
  );
}
