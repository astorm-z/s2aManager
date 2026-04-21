function resolveTarget(selector) {
  if (!selector) return null;
  return document.querySelector(selector);
}

const defaultCredentialsJson = JSON.stringify(
  {
    model_mapping: {
      "gpt-5.2": "gpt-5.2",
      "gpt-5.3-codex": "gpt-5.3-codex",
      "gpt-5.4": "gpt-5.4",
      "gpt-5.4-mini": "gpt-5.4-mini",
    },
  },
  null,
  2,
);

async function requestFragment(url, options, targetSelector) {
  const target = resolveTarget(targetSelector);
  if (!target) return;
  target.innerHTML = '<div class="loading-state">处理中...</div>';
  try {
    const response = await fetch(url, options);
    const text = await response.text();
    target.innerHTML = text;
    target.querySelectorAll("[data-refresh-target][data-refresh-url]").forEach((node) => {
      const refreshTarget = node.dataset.refreshTarget;
      const refreshUrl = node.dataset.refreshUrl;
      if (refreshTarget && refreshUrl) {
        requestFragment(refreshUrl, { method: "GET", headers: { "X-Requested-With": "fetch" } }, refreshTarget);
      }
    });
  } catch (error) {
    target.innerHTML = '<div class="alert danger">请求失败，请稍后重试。</div>';
    console.error(error);
  }
}

function applySubmitterFormValue(submitter) {
  const targetSelector = submitter?.dataset.formValueTarget;
  if (!targetSelector) return;
  const target = resolveTarget(targetSelector);
  if (!target || !("value" in target)) return;
  target.value = submitter.dataset.formValue || "";
}

function resolveSubmitAction(form, submitter) {
  const submitterAction = submitter?.getAttribute("formaction");
  if (submitterAction) return submitterAction;
  const formAction = form.getAttribute("action");
  if (formAction) return formAction;
  return window.location.href;
}

function resolveSubmitMethod(form, submitter) {
  const submitterMethod = submitter?.getAttribute("formmethod");
  if (submitterMethod) return submitterMethod.toUpperCase();
  const formMethod = form.getAttribute("method");
  if (formMethod) return formMethod.toUpperCase();
  return "GET";
}

document.addEventListener("submit", async (event) => {
  const form = event.target;
  const submitter = event.submitter;
  const targetSelector = submitter?.dataset.partialTarget || form.dataset.partialTarget;
  if (!targetSelector) return;
  event.preventDefault();
  const confirmMessage = submitter?.dataset.confirm || form.dataset.confirm;
  if (confirmMessage && !window.confirm(confirmMessage)) {
    return;
  }
  applySubmitterFormValue(submitter);

  const action = resolveSubmitAction(form, submitter);
  const method = resolveSubmitMethod(form, submitter);
  if (method === "GET") {
    const params = new URLSearchParams(new FormData(form));
    await requestFragment(`${action}?${params.toString()}`, { method: "GET", headers: { "X-Requested-With": "fetch" } }, targetSelector);
    return;
  }

  await requestFragment(
    action,
    {
      method,
      body: new FormData(form),
      headers: { "X-Requested-With": "fetch" },
    },
    targetSelector,
  );
});

function activateTab(tabName) {
  document.querySelectorAll("[data-tab-trigger]").forEach((button) => {
    button.classList.toggle("active", button.dataset.tabTrigger === tabName);
  });
  document.querySelectorAll("[data-tab-panel]").forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.tabPanel === tabName);
  });
  document.querySelectorAll("[data-active-tab-input]").forEach((input) => {
    input.value = tabName;
  });
}

function refreshScheduleModeUi(select) {
  if (!select) return;
  const mode = select.value || "immediate";
  const root = select.closest(".collapse-body") || document;
  root.querySelectorAll("[data-schedule-panel]").forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.schedulePanel === mode);
  });
}

document.addEventListener("click", (event) => {
  const fillButton = event.target.closest("[data-fill-default-target]");
  if (fillButton) {
    event.preventDefault();
    event.stopPropagation();
    const target = resolveTarget(fillButton.dataset.fillDefaultTarget);
    if (target && "value" in target) {
      target.value = defaultCredentialsJson;
      target.dispatchEvent(new Event("input", { bubbles: true }));
      target.dispatchEvent(new Event("change", { bubbles: true }));
      target.focus();
    }
    return;
  }

  const button = event.target.closest("[data-tab-trigger]");
  if (!button) return;
  activateTab(button.dataset.tabTrigger);
});

document.addEventListener("change", (event) => {
  const select = event.target.closest("[data-schedule-mode-select]");
  if (!select) return;
  refreshScheduleModeUi(select);
});

document.addEventListener("DOMContentLoaded", () => {
  activateTab(document.body.dataset.defaultTab || "manage");
  document.querySelectorAll("[data-schedule-mode-select]").forEach((select) => {
    refreshScheduleModeUi(select);
  });
});
