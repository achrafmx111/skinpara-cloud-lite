const dns = require('node:dns/promises');

const ALLOWED_INTERNAL_HOSTS = [
  'rails',
  // Loopback is required only for the embedded catalog service in this container.
  '127.0.0.1',
  'localhost',
  'skinpara-ai-bridge',
  'skinpara-catalog-service',
  'skinpara-control-center',
  'skinpara-auth-gateway'
];

const ALLOWED_EXTERNAL_DOMAINS = [
  '.myshopify.com', 
  'api.openai.com', 
  'openrouter.ai', 
  'supabase.co'
];

function isPrivateIP(ip) {
  const parts = ip.split('.').map(Number);
  if (parts.length !== 4) return false;
  if (parts[0] === 10) return true;
  if (parts[0] === 127) return true;
  if (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) return true;
  if (parts[0] === 192 && parts[1] === 168) return true;
  if (parts[0] === 169 && parts[1] === 254) return true; // cloud metadata
  return false;
}

function isValidHost(host) {
  if (ALLOWED_INTERNAL_HOSTS.includes(host)) return true;
  for (const domain of ALLOWED_EXTERNAL_DOMAINS) {
    if (host === domain || host.endsWith(domain)) {
      return true;
    }
  }
  return false;
}

async function safeFetch(url, options = {}, hop = 0) {
  if (hop > 3) {
    throw new Error('SSRF Blocked: Too many redirects');
  }

  const parsedUrl = new URL(url);
  
  if (parsedUrl.protocol !== 'http:' && parsedUrl.protocol !== 'https:') {
    throw new Error(`SSRF Blocked: Invalid protocol ${parsedUrl.protocol}`);
  }

  const host = parsedUrl.hostname;
  
  if (!isValidHost(host)) {
    throw new Error(`SSRF Blocked: Host ${host} is not in the allowlist.`);
  }

  // Check DNS resolution for private IPs unless it's a known internal docker host
  if (!ALLOWED_INTERNAL_HOSTS.includes(host)) {
    try {
      const addresses = await dns.resolve4(host);
      if (addresses.some(isPrivateIP)) {
        throw new Error(`SSRF Blocked: Host ${host} resolves to private IP.`);
      }
    } catch (err) {
      throw new Error(`SSRF Blocked: DNS resolution failed for ${host}`);
    }
  }

  const controller = new AbortController();
  const timeoutMs = options.timeout || 30000;
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  const fetchOptions = {
    ...options,
    signal: controller.signal,
    redirect: 'manual',
    size: 5 * 1024 * 1024 // 5MB bound size where supported by fetch API (like node-fetch)
  };

  try {
    const response = await fetch(url, fetchOptions);
    
    // Handle redirect safely
    if (response.status >= 300 && response.status < 400 && response.headers.has('location')) {
      const redirectLocation = response.headers.get('location');
      const redirectUrl = new URL(redirectLocation, url).toString();
      return safeFetch(redirectUrl, { ...options, method: 'GET', body: undefined }, hop + 1);
    }

    return response;
  } catch (error) {
    if (error.name === 'AbortError') {
      throw new Error(`safeFetch timeout after ${timeoutMs}ms`);
    }
    // Redact any URL/credentials if they leaked into error message
    throw new Error(`safeFetch error: ${error.message.replace(/([a-zA-Z0-9_-]+:[a-zA-Z0-9_-]+@)/g, '***:***@')}`);
  } finally {
    clearTimeout(timeoutId);
  }
}



module.exports = { safeFetch };

