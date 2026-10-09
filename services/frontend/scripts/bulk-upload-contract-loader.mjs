const clientStub = `
const client = globalThis.__bulkUploadTestClient ??= {
  calls: [],
  post: async (...args) => { client.calls.push(['post', ...args]); return { data: {} }; },
  get: async (...args) => { client.calls.push(['get', ...args]); return { data: {} }; },
  delete: async (...args) => { client.calls.push(['delete', ...args]); return { data: {} }; },
};
export default client;
`;

export async function resolve(specifier, context, nextResolve) {
  if (specifier === './client.ts' && context.parentURL?.endsWith('/src/api/bulkUpload.ts')) {
    return {
      url: `data:text/javascript,${encodeURIComponent(clientStub)}`,
      shortCircuit: true,
    };
  }
  return nextResolve(specifier, context);
}
