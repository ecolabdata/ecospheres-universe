const POOL_SIZE = 10;


ready(() => {
  grist.ready({requiredAccess: "full"});
});


function ready(fn) {
  if (document.readyState !== "loading") {
    fn();
  } else {
    document.addEventListener("DOMContentLoaded", fn);
  }
}


async function handleClick(btn) {
  btn.innerHTML = 'En cours... <span class="spinner"></span>';
  btn.disabled = true;

  try {
    await synchronize();
  } catch (err) {
    console.error("DatagouvSync:", err);
  }

  btn.innerHTML = "Synchroniser";
  btn.disabled = false;
}


async function synchronize() {
  console.log(`DatagouvSync: Synchronising...`);

  const tableId = await grist.selectedTable.getTableId();
  const env = tableId.toLowerCase().startsWith("prod") ? "www" : "demo";

  const data = await grist.docApi.fetchTable(tableId);
  if (data.id.length == 0) {
    console.log(`DatagouvSync: Nothing in grist`);
    return;
  }

  const rows = cols2rows(data);
  const results = await pooled(POOL_SIZE, rows, row => resolve(env, row));
  const resolved = results.filter(Boolean);
  if (resolved.length == 0) {
    console.log(`DatagouvSync: Nothing to update`);
    return;
  }

  const cols = rows2cols(resolved);
  try {
    await grist.docApi.applyUserActions([
      ["BulkUpdateRecord", tableId, cols.id, { Label: cols.Label, URL: cols.URL }]
    ]);
    console.log(`DatagouvSync: Updated ${cols.id.length} row(s)`);
  } catch (err) {
    console.error("DatagouvSync: Failed to update table:", err);
  }
}


async function resolve(env, row) {
  const type = row.Type.trim().toLowerCase();
  if (type == "tag") {
    return;
  }
  const identifier = row.Identifiant.trim();
  const object = `${type}s`
  const version = type == "topic" ? "2" : "1";

  let result;
  try {
    const response = await fetch(
      `https://${env}.data.gouv.fr/api/${version}/${object}/${identifier}/`,
      {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          "X-Fields": "name,page,self_web_url,title"
        }
      }
    );
    if (!response.ok) {
      console.warn(`DatagouvSync: Failed request for ${object}/${identifier}: ${response.statusText || response.status}`);
      return;
    }
    result = await response.json();
  } catch (err) {
    console.error(`DatagouvSync: Error processing ${object}/${identifier}:`, err);
    return;
  }

  // fields used here must be declared in the X-Fields request header above
  const label = result.name || result.title || "<missing>";
  const page = result.page || result.self_web_url || "<missing>";
  // FIXME: page URL for topics?

  console.log(`DatagouvSync: Found ${object}/${identifier}: label="${label}", page=${page}`);
  return {id: row.id, Label: label, URL: page};
}


function cols2rows(cols) {
  return Object.values(cols)[0].map((_, i) =>
    Object.fromEntries(
      Object.entries(cols).map(([col, values]) => [col, values[i]])
    )
  );
}


function rows2cols(rows) {
  return Object.fromEntries(
    Object.keys(rows[0]).map(col => [col, rows.map(row => row[col])])
  );
}


async function pooled(limit, array, fn) {
  const results = [];
  const executing = new Set();

  for (const item of array) {
    const p = Promise.resolve().then(() => fn(item));
    results.push(p);
    executing.add(p);
    p.finally(() => executing.delete(p));
    if (executing.size >= limit) await Promise.race(executing);
  }

  return Promise.all(results);
}
