async function getHealth() {
  const base = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
  try {
    const res = await fetch(`${base}/health`, { cache: 'no-store' });
    if (!res.ok) return null;
    return res.json();
  } catch {
    return null;
  }
}

export default async function Page() {
  const health = await getHealth();
  return (
    <main>
      <h1>Monorepo Next.js app</h1>
      <p>API Health:</p>
      <pre>{JSON.stringify(health ?? { status: 'unreachable' }, null, 2)}</pre>
    </main>
  );
}
