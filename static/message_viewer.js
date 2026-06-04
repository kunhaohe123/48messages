document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll("[data-clearable-date]").forEach((input) => {
    const wrapper = input.closest(".date-input-wrap");
    if (!wrapper) {
      return;
    }

    const clearButton = document.createElement("button");
    clearButton.type = "button";
    clearButton.className = "date-clear";
    clearButton.textContent = "x";
    clearButton.title = "清空日期";
    clearButton.setAttribute("aria-label", "清空日期");
    wrapper.appendChild(clearButton);

    const sync = () => {
      clearButton.style.display = input.value ? "flex" : "none";
    };

    clearButton.addEventListener("click", () => {
      input.value = "";
      input.dispatchEvent(new Event("input", { bubbles: true }));
      input.dispatchEvent(new Event("change", { bubbles: true }));
      input.focus();
      sync();
    });

    input.addEventListener("input", sync);
    input.addEventListener("change", sync);
    sync();
  });
});
