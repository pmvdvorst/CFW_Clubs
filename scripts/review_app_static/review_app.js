(function () {
  function postForm(url, formData) {
    return fetch(url, {
      method: "POST",
      body: formData,
      headers: {
        "X-Requested-With": "fetch",
      },
      credentials: "same-origin",
    });
  }

  function initDiscoveryPolling() {
    const panel = document.querySelector("[data-discovery-poll]");
    if (!panel) {
      return;
    }
    const statusUrl = panel.getAttribute("data-status-url");
    if (!statusUrl) {
      return;
    }

    const statusNode = panel.querySelector("[data-discovery-status]");
    const startedNode = panel.querySelector("[data-discovery-started]");
    const finishedNode = panel.querySelector("[data-discovery-finished]");
    const returnNode = panel.querySelector("[data-discovery-return]");
    const commandNode = panel.querySelector("[data-discovery-command]");
    const logNode = panel.querySelector("[data-discovery-log]");

    async function refresh() {
      const response = await fetch(statusUrl, { credentials: "same-origin" });
      if (!response.ok) {
        return;
      }
      const payload = await response.json();
      if (statusNode) {
        statusNode.textContent = payload.status || "idle";
      }
      if (startedNode) {
        startedNode.textContent = payload.started_at || "Not started";
      }
      if (finishedNode) {
        finishedNode.textContent = payload.finished_at || (payload.running ? "Still running" : "Not finished");
      }
      if (returnNode) {
        returnNode.textContent = payload.return_code === null ? "Pending" : String(payload.return_code);
      }
      if (commandNode) {
        commandNode.textContent = payload.command && payload.command.length ? payload.command.join(" ") : "No run yet.";
      }
      if (logNode) {
        logNode.textContent = payload.log || "No logs yet.";
        logNode.scrollTop = logNode.scrollHeight;
      }
    }

    refresh();
    window.setInterval(refresh, 2000);
  }

  function initSelectionCheckboxes() {
    const checkboxes = document.querySelectorAll(".selection-checkbox");
    checkboxes.forEach((checkbox) => {
      checkbox.addEventListener("change", async () => {
        const url = checkbox.getAttribute("data-selection-url");
        const stagingCsv = checkbox.getAttribute("data-staging-csv");
        if (!url || !stagingCsv) {
          return;
        }
        const formData = new FormData();
        formData.set("staging_csv", stagingCsv);
        formData.set("row_id", checkbox.value);
        formData.set("selected", checkbox.checked ? "1" : "0");
        try {
          await postForm(url, formData);
        } catch (error) {
          checkbox.checked = !checkbox.checked;
          window.alert("Could not update the export selection.");
        }
      });
    });
  }

  function initBulkActions() {
    const form = document.querySelector("[data-bulk-form]");
    if (!form) {
      return;
    }
    const actionInput = form.querySelector('input[name="action"]');
    const rowIdsInput = form.querySelector('input[name="row_ids"]');
    const actionButtons = form.querySelectorAll("[data-bulk-action]");

    function visibleRowIds() {
      return Array.from(document.querySelectorAll(".selection-checkbox")).map((checkbox) => checkbox.value);
    }

    function checkedRowIds() {
      return Array.from(document.querySelectorAll(".selection-checkbox:checked")).map((checkbox) => checkbox.value);
    }

    actionButtons.forEach((button) => {
      button.addEventListener("click", () => {
        const action = button.getAttribute("data-bulk-action") || "";
        let rowIds = [];
        if (action === "select_visible") {
          rowIds = visibleRowIds();
        } else if (action === "clear_selection") {
          rowIds = [];
        } else {
          rowIds = checkedRowIds();
          if (!rowIds.length) {
            window.alert("Choose at least one checked row for that bulk action.");
            return;
          }
        }
        actionInput.value = action;
        rowIdsInput.value = rowIds.join(",");
        form.submit();
      });
    });
  }

  function initAutosaveForms() {
    const forms = document.querySelectorAll(".js-autosave-form");
    forms.forEach((form) => {
      const statusNode = form.querySelector("[data-autosave-status]");
      const autosaveUrl = form.getAttribute("data-autosave-url") || form.getAttribute("action");
      let timer = null;

      function setStatus(text, className) {
        if (!statusNode) {
          return;
        }
        statusNode.textContent = text;
        statusNode.classList.remove("is-saving", "is-success", "is-error");
        if (className) {
          statusNode.classList.add(className);
        }
      }

      async function save() {
        if (!autosaveUrl) {
          return;
        }
        setStatus("Saving changes…", "is-saving");
        try {
          const response = await postForm(autosaveUrl, new FormData(form));
          const payload = await response.json();
          if (!response.ok || !payload.ok) {
            throw new Error(payload.error || "Could not save that row.");
          }
          setStatus(`Saved at ${payload.last_checked || "just now"}.`, "is-success");
        } catch (error) {
          setStatus(error.message || "Could not save that row.", "is-error");
        }
      }

      form.addEventListener("input", () => {
        window.clearTimeout(timer);
        timer = window.setTimeout(save, 600);
      });
      form.addEventListener("change", () => {
        window.clearTimeout(timer);
        timer = window.setTimeout(save, 300);
      });
    });
  }

  function initInlineRowEditors() {
    const rows = document.querySelectorAll("[data-inline-row-editor]");
    const mediaFieldNames = new Set(["logo_source_url", "cover_source_url", "gallery_source_urls"]);

    function cleanValue(value) {
      return (value || "").trim();
    }

    function parseGalleryValue(value) {
      return (value || "")
        .split(/\n|::/g)
        .map((item) => item.trim())
        .filter(Boolean);
    }

    function buildPhotoItems(coverValue, galleryValue) {
      const photoItems = [];
      const seen = new Set();
      const coverUrl = cleanValue(coverValue);
      if (coverUrl) {
        seen.add(coverUrl);
        photoItems.push({ url: coverUrl, kind: "cover", label: "Cover" });
      }
      let galleryIndex = 0;
      parseGalleryValue(galleryValue).forEach((url) => {
        if (!url || seen.has(url)) {
          return;
        }
        seen.add(url);
        photoItems.push({ url, kind: "gallery", label: "Picture", galleryIndex });
        galleryIndex += 1;
      });
      return photoItems;
    }

    function setStatus(node, text, className) {
      if (!node) {
        return;
      }
      node.textContent = text;
      node.classList.remove("is-saving", "is-success", "is-error");
      if (className) {
        node.classList.add(className);
      }
    }

    function syncSelectedDetail(rowId, name, value) {
      const detailForm = document.querySelector(`.js-autosave-form[data-row-id="${rowId}"]`);
      if (!detailForm) {
        return;
      }
      const field = detailForm.querySelector(`[name="${name}"]`);
      if (!field) {
        return;
      }
      field.value = value;
      if (mediaFieldNames.has(name)) {
        field.dispatchEvent(new Event("row-sync"));
      }
      if (name === "club_name") {
        const title = document.querySelector("[data-detail-title]");
        if (title) {
          title.textContent = value || title.getAttribute("data-default-title") || rowId;
        }
      }
      if (name === "website") {
        const website = document.querySelector("[data-detail-website]");
        if (website) {
          website.textContent = value || website.getAttribute("data-source-url") || "No website yet";
        }
      }
    }

    rows.forEach((row) => {
      const statusNode = row.querySelector("[data-inline-status]");
      const autosaveUrl = row.getAttribute("data-autosave-url");
      const rowId = row.getAttribute("data-row-id") || "";
      const fields = Array.from(row.querySelectorAll("input[name], select[name], textarea[name]"));
      const clubNameField = row.querySelector('input[name="club_name"]');
      const logoField = row.querySelector('input[name="logo_source_url"]');
      const coverField = row.querySelector('input[name="cover_source_url"]');
      const galleryField = row.querySelector('input[name="gallery_source_urls"]');
      let timer = null;

      if (!autosaveUrl || !fields.length) {
        return;
      }

      function buildLogoPreview(state) {
        const logoSlot = row.querySelector("[data-table-logo-slot]");
        if (!logoSlot) {
          return;
        }
        logoSlot.replaceChildren();
        if (state.logoUrl) {
          const figure = document.createElement("figure");
          figure.className = "media-card table-media-card table-logo-card";

          const deleteButton = document.createElement("button");
          deleteButton.type = "button";
          deleteButton.className = "media-action-button";
          deleteButton.textContent = "x";
          deleteButton.setAttribute("data-table-delete-logo", "1");
          deleteButton.setAttribute("aria-label", "Delete logo");
          if (logoField && logoField.disabled) {
            deleteButton.disabled = true;
          }
          figure.appendChild(deleteButton);

          const thumb = document.createElement("div");
          thumb.className = "media-thumb media-thumb-logo";
          const image = document.createElement("img");
          image.src = state.logoUrl;
          image.alt = `Logo for ${state.clubLabel}`;
          image.loading = "lazy";
          thumb.appendChild(image);
          figure.appendChild(thumb);
          logoSlot.appendChild(figure);
          return;
        }

        const empty = document.createElement("div");
        empty.className = "media-empty table-media-empty";
        empty.textContent = "No logo";
        logoSlot.appendChild(empty);

        const addButton = document.createElement("button");
        addButton.type = "button";
        addButton.className = "button button-small";
        addButton.textContent = "+ Add logo";
        addButton.setAttribute("data-table-add-logo", "1");
        if (logoField && logoField.disabled) {
          addButton.disabled = true;
        }
        logoSlot.appendChild(addButton);
      }

      function buildPhotoPreview(state) {
        const photoSlot = row.querySelector("[data-table-photo-slot]");
        if (!photoSlot) {
          return;
        }
        photoSlot.replaceChildren();
        if (!state.photoItems.length) {
          const empty = document.createElement("div");
          empty.className = "media-empty table-media-empty";
          empty.textContent = "No photos";
          photoSlot.appendChild(empty);
          return;
        }

        photoSlot.setAttribute("aria-label", `Photo previews for ${state.clubLabel}`);
        state.photoItems.forEach((photo) => {
          const figure = document.createElement("figure");
          figure.className = "media-card table-media-card";
          figure.setAttribute("role", "listitem");

          const deleteButton = document.createElement("button");
          deleteButton.type = "button";
          deleteButton.className = "media-action-button";
          deleteButton.textContent = "x";
          deleteButton.setAttribute("data-table-delete-photo-kind", photo.kind);
          deleteButton.setAttribute("aria-label", `Delete ${photo.label.toLowerCase()}`);
          if (photo.kind === "gallery") {
            deleteButton.setAttribute("data-table-delete-photo-index", String(photo.galleryIndex));
          }
          if ((coverField && coverField.disabled) || (galleryField && galleryField.disabled)) {
            deleteButton.disabled = true;
          }
          figure.appendChild(deleteButton);

          const thumb = document.createElement("div");
          thumb.className = "media-thumb media-thumb-photo";
          const image = document.createElement("img");
          image.src = photo.url;
          image.alt = `${photo.label} image for ${state.clubLabel}`;
          image.loading = "lazy";
          image.setAttribute("data-preview-image", "1");
          thumb.appendChild(image);
          figure.appendChild(thumb);

          const meta = document.createElement("figcaption");
          meta.className = "media-meta";
          meta.setAttribute("data-image-size", "1");
          figure.appendChild(meta);

          photoSlot.appendChild(figure);
        });
      }

      function currentMediaState() {
        return {
          clubLabel: cleanValue(clubNameField ? clubNameField.value : "") || rowId,
          logoUrl: cleanValue(logoField ? logoField.value : ""),
          photoItems: buildPhotoItems(coverField ? coverField.value : "", galleryField ? galleryField.value : ""),
        };
      }

      function renderTableMedia() {
        const state = currentMediaState();
        buildLogoPreview(state);
        buildPhotoPreview(state);
        bindImageDimensions(row);
      }

      function setFieldValue(field, value) {
        if (!field) {
          return;
        }
        if (field.value === value) {
          renderTableMedia();
          return;
        }
        field.value = value;
        field.dispatchEvent(new Event("input", { bubbles: true }));
      }

      function addLogo() {
        const url = cleanValue(window.prompt("Paste the logo image URL."));
        if (!url) {
          return;
        }
        setFieldValue(logoField, url);
      }

      function addPhoto() {
        const url = cleanValue(window.prompt("Paste the picture image URL."));
        if (!url) {
          return;
        }
        const coverUrl = cleanValue(coverField ? coverField.value : "");
        const galleryUrls = parseGalleryValue(galleryField ? galleryField.value : "");
        if (url === coverUrl || galleryUrls.includes(url)) {
          window.alert("That picture URL is already on this row.");
          return;
        }
        if (!coverUrl && !galleryUrls.length) {
          setFieldValue(coverField, url);
          return;
        }
        galleryUrls.push(url);
        setFieldValue(galleryField, galleryUrls.join("\n"));
      }

      function deletePhoto(kind, indexValue) {
        if (kind === "cover") {
          setFieldValue(coverField, "");
          return;
        }
        const galleryUrls = parseGalleryValue(galleryField ? galleryField.value : "");
        const index = Number(indexValue);
        if (Number.isNaN(index) || index < 0 || index >= galleryUrls.length) {
          return;
        }
        galleryUrls.splice(index, 1);
        setFieldValue(galleryField, galleryUrls.join("\n"));
      }

      async function save() {
        const formData = new FormData();
        fields.forEach((field) => {
          if (field.disabled || !field.name) {
            return;
          }
          formData.set(field.name, field.value);
        });
        setStatus(statusNode, "Saving changes…", "is-saving");
        try {
          const response = await postForm(autosaveUrl, formData);
          const payload = await response.json();
          if (!response.ok || !payload.ok) {
            throw new Error(payload.error || "Could not save that row.");
          }
          setStatus(statusNode, `Saved at ${payload.last_checked || "just now"}.`, "is-success");
        } catch (error) {
          setStatus(statusNode, error.message || "Could not save that row.", "is-error");
        }
      }

      function queueSave(delay) {
        window.clearTimeout(timer);
        timer = window.setTimeout(save, delay);
      }

      fields.forEach((field) => {
        if (field.name === "staging_csv") {
          return;
        }
        const sync = () => {
          syncSelectedDetail(rowId, field.name, field.value);
        };
        field.addEventListener("input", () => {
          sync();
          if (mediaFieldNames.has(field.name) || field.name === "club_name") {
            renderTableMedia();
          }
          queueSave(600);
        });
        field.addEventListener("change", () => {
          sync();
          if (mediaFieldNames.has(field.name) || field.name === "club_name") {
            renderTableMedia();
          }
          queueSave(300);
        });
      });

      row.addEventListener("click", (event) => {
        const deleteLogoButton = event.target.closest("[data-table-delete-logo]");
        if (deleteLogoButton) {
          setFieldValue(logoField, "");
          return;
        }
        const addLogoButton = event.target.closest("[data-table-add-logo]");
        if (addLogoButton) {
          addLogo();
          return;
        }
        const addPhotoButton = event.target.closest("[data-table-add-photo]");
        if (addPhotoButton) {
          addPhoto();
          return;
        }
        const deletePhotoButton = event.target.closest("[data-table-delete-photo-kind]");
        if (!deletePhotoButton) {
          return;
        }
        deletePhoto(
          deletePhotoButton.getAttribute("data-table-delete-photo-kind") || "",
          deletePhotoButton.getAttribute("data-table-delete-photo-index")
        );
      });

      renderTableMedia();
    });
  }

  function bindImageDimensions(root) {
    const images = (root || document).querySelectorAll("[data-preview-image]");

    function updateLabel(image) {
      const label = image.closest(".media-card")?.querySelector("[data-image-size]");
      if (!label) {
        return;
      }
      if (image.naturalWidth && image.naturalHeight) {
        label.textContent = `${image.naturalWidth}×${image.naturalHeight}`;
      } else {
        label.textContent = "";
      }
    }

    images.forEach((image) => {
      if (image.complete) {
        updateLabel(image);
        return;
      }
      image.addEventListener("load", () => updateLabel(image), { once: true });
      image.addEventListener("error", () => updateLabel(image), { once: true });
    });
  }

  function initMediaEditors() {
    const editors = document.querySelectorAll("[data-media-editor]");

    function cleanValue(value) {
      return (value || "").trim();
    }

    function parseGalleryValue(value) {
      return (value || "")
        .split(/\n|::/g)
        .map((item) => item.trim())
        .filter(Boolean);
    }

    function buildPhotoItems(coverValue, galleryValue) {
      const photoItems = [];
      const seen = new Set();
      const coverUrl = cleanValue(coverValue);
      if (coverUrl) {
        seen.add(coverUrl);
        photoItems.push({ url: coverUrl, kind: "cover", label: "Cover" });
      }
      let galleryIndex = 0;
      parseGalleryValue(galleryValue).forEach((url) => {
        if (!url || seen.has(url)) {
          return;
        }
        seen.add(url);
        photoItems.push({ url, kind: "gallery", label: "Picture", galleryIndex });
        galleryIndex += 1;
      });
      return photoItems;
    }

    function buildEmptyState(message) {
      const empty = document.createElement("div");
      empty.className = "media-empty";
      empty.textContent = message;
      return empty;
    }

    function buildMediaCard(config) {
      const figure = document.createElement("figure");
      figure.className = config.cardClassName || "media-card detail-media-card";

      const deleteButton = document.createElement("button");
      deleteButton.type = "button";
      deleteButton.className = "media-action-button";
      deleteButton.textContent = "x";
      deleteButton.setAttribute("aria-label", config.deleteLabel);
      if (config.deleteKind === "logo") {
        deleteButton.setAttribute("data-delete-logo", "1");
      } else {
        deleteButton.setAttribute("data-delete-photo-kind", config.deleteKind);
        if (typeof config.galleryIndex === "number") {
          deleteButton.setAttribute("data-delete-photo-index", String(config.galleryIndex));
        }
      }
      if (config.disabled) {
        deleteButton.disabled = true;
      }
      figure.appendChild(deleteButton);

      const thumb = document.createElement("div");
      thumb.className = config.thumbClassName;
      const image = document.createElement("img");
      image.src = config.url;
      image.alt = config.alt;
      image.loading = "lazy";
      image.setAttribute("data-preview-image", "1");
      thumb.appendChild(image);
      figure.appendChild(thumb);

      const caption = document.createElement("figcaption");
      caption.className = "media-card-caption";

      const badge = document.createElement("span");
      badge.className = "media-kind-badge";
      badge.textContent = config.badgeLabel;
      caption.appendChild(badge);

      const meta = document.createElement("span");
      meta.className = "media-meta";
      meta.setAttribute("data-image-size", "1");
      caption.appendChild(meta);

      figure.appendChild(caption);
      return figure;
    }

    function buildTableLogoPreview(logoUrl, clubLabel, disabled) {
      const wrapper = document.createElement("div");
      wrapper.className = "table-media-stack";
      wrapper.setAttribute("data-table-logo-slot", "1");

      if (logoUrl) {
        const figure = document.createElement("figure");
        figure.className = "media-card table-media-card table-logo-card";

        const deleteButton = document.createElement("button");
        deleteButton.type = "button";
        deleteButton.className = "media-action-button";
        deleteButton.textContent = "x";
        deleteButton.setAttribute("data-table-delete-logo", "1");
        deleteButton.setAttribute("aria-label", "Delete logo");
        if (disabled) {
          deleteButton.disabled = true;
        }
        figure.appendChild(deleteButton);

        const thumb = document.createElement("div");
        thumb.className = "media-thumb media-thumb-logo";
        const image = document.createElement("img");
        image.src = logoUrl;
        image.alt = `Logo for ${clubLabel}`;
        image.loading = "lazy";
        thumb.appendChild(image);
        figure.appendChild(thumb);
        wrapper.appendChild(figure);
        return wrapper;
      }

      const empty = document.createElement("div");
      empty.className = "media-empty table-media-empty";
      empty.textContent = "No logo";
      wrapper.appendChild(empty);

      const addButton = document.createElement("button");
      addButton.type = "button";
      addButton.className = "button button-small";
      addButton.textContent = "+ Add logo";
      addButton.setAttribute("data-table-add-logo", "1");
      if (disabled) {
        addButton.disabled = true;
      }
      wrapper.appendChild(addButton);
      return wrapper;
    }

    function buildTablePhotoPreview(photoItems, clubLabel, disabled) {
      const wrapper = document.createElement("div");
      wrapper.className = "table-media-stack";

      const strip = document.createElement("div");
      strip.className = "media-strip";
      strip.setAttribute("data-table-photo-slot", "1");
      strip.setAttribute("role", "list");
      strip.setAttribute("aria-label", `Photo previews for ${clubLabel}`);

      if (!photoItems.length) {
        const empty = document.createElement("div");
        empty.className = "media-empty table-media-empty";
        empty.textContent = "No photos";
        strip.appendChild(empty);
      } else {
        photoItems.forEach((photo) => {
          const figure = document.createElement("figure");
          figure.className = "media-card table-media-card";
          figure.setAttribute("role", "listitem");

          const deleteButton = document.createElement("button");
          deleteButton.type = "button";
          deleteButton.className = "media-action-button";
          deleteButton.textContent = "x";
          deleteButton.setAttribute("data-table-delete-photo-kind", photo.kind);
          deleteButton.setAttribute("aria-label", `Delete ${photo.label.toLowerCase()}`);
          if (photo.kind === "gallery") {
            deleteButton.setAttribute("data-table-delete-photo-index", String(photo.galleryIndex));
          }
          if (disabled) {
            deleteButton.disabled = true;
          }
          figure.appendChild(deleteButton);

          const thumb = document.createElement("div");
          thumb.className = "media-thumb media-thumb-photo";
          const image = document.createElement("img");
          image.src = photo.url;
          image.alt = `${photo.label} image for ${clubLabel}`;
          image.loading = "lazy";
          image.setAttribute("data-preview-image", "1");
          thumb.appendChild(image);
          figure.appendChild(thumb);

          const meta = document.createElement("figcaption");
          meta.className = "media-meta";
          meta.setAttribute("data-image-size", "1");
          figure.appendChild(meta);

          strip.appendChild(figure);
        });
      }

      wrapper.appendChild(strip);

      const addButton = document.createElement("button");
      addButton.type = "button";
      addButton.className = "button button-small";
      addButton.textContent = "+ Add picture";
      addButton.setAttribute("data-table-add-photo", "1");
      if (disabled) {
        addButton.disabled = true;
      }
      wrapper.appendChild(addButton);
      return wrapper;
    }

    editors.forEach((editor) => {
      const form = editor.closest("form");
      if (!form) {
        return;
      }
      const rowId = editor.getAttribute("data-row-id") || "";
      const logoField = form.querySelector('input[name="logo_source_url"]');
      const coverField = form.querySelector('input[name="cover_source_url"]');
      const galleryField = form.querySelector('textarea[name="gallery_source_urls"]');
      const clubNameField = form.querySelector('input[name="club_name"]');
      const logoSlot = editor.querySelector("[data-logo-slot]");
      const photoSlot = editor.querySelector("[data-photo-slot]");
      const addLogoButton = editor.querySelector("[data-add-logo]");
      const addPhotoButton = editor.querySelector("[data-add-photo]");
      const readOnly = Boolean((logoField && logoField.disabled) || (coverField && coverField.disabled) || (galleryField && galleryField.disabled));

      if (!logoField || !coverField || !galleryField || !logoSlot || !photoSlot) {
        return;
      }

      function currentState() {
        return {
          clubLabel: cleanValue(clubNameField ? clubNameField.value : "") || "this club",
          logoUrl: cleanValue(logoField.value),
          photoItems: buildPhotoItems(coverField.value, galleryField.value),
        };
      }

      function syncTablePreview(state) {
        if (!rowId) {
          return;
        }
        const row = document.querySelector(`tr[data-row-id="${rowId}"]`);
        if (!row) {
          return;
        }
        const logoCell = row.querySelector("[data-table-logo]");
        const photoCell = row.querySelector("[data-table-photos]");
        if (logoCell) {
          logoCell.replaceChildren(buildTableLogoPreview(state.logoUrl, state.clubLabel, readOnly));
        }
        if (photoCell) {
          photoCell.replaceChildren(buildTablePhotoPreview(state.photoItems, state.clubLabel, readOnly));
        }
        bindImageDimensions(row);
      }

      function render() {
        const state = currentState();
        logoSlot.replaceChildren();
        if (state.logoUrl) {
          logoSlot.appendChild(
            buildMediaCard({
              url: state.logoUrl,
              alt: `Logo for ${state.clubLabel}`,
              badgeLabel: "Logo",
              deleteLabel: "Delete logo",
              deleteKind: "logo",
              thumbClassName: "media-thumb media-thumb-logo detail-media-thumb-logo",
              cardClassName: "media-card detail-media-card detail-media-card-logo",
              disabled: readOnly,
            })
          );
        } else {
          logoSlot.appendChild(buildEmptyState("No logo yet."));
        }

        photoSlot.replaceChildren();
        if (state.photoItems.length) {
          state.photoItems.forEach((photo) => {
            photoSlot.appendChild(
              buildMediaCard({
                url: photo.url,
                alt: `${photo.label} image for ${state.clubLabel}`,
                badgeLabel: photo.label,
                deleteLabel: `Delete ${photo.label.toLowerCase()}`,
                deleteKind: photo.kind,
                galleryIndex: photo.kind === "gallery" ? photo.galleryIndex : null,
                thumbClassName: "media-thumb media-thumb-photo",
                cardClassName: "media-card detail-media-card",
                disabled: readOnly,
              })
            );
          });
        } else {
          photoSlot.appendChild(buildEmptyState("No pictures yet."));
        }

        if (addLogoButton) {
          addLogoButton.hidden = Boolean(state.logoUrl);
        }

        bindImageDimensions(editor);
        syncTablePreview(state);
      }

      function setFieldValue(field, value) {
        if (field.value === value) {
          render();
          return;
        }
        field.value = value;
        field.dispatchEvent(new Event("input", { bubbles: true }));
      }

      function addLogo() {
        const url = cleanValue(window.prompt("Paste the logo image URL."));
        if (!url) {
          return;
        }
        setFieldValue(logoField, url);
      }

      function addPhoto() {
        const url = cleanValue(window.prompt("Paste the picture image URL."));
        if (!url) {
          return;
        }
        const coverUrl = cleanValue(coverField.value);
        const galleryUrls = parseGalleryValue(galleryField.value);
        if (url === coverUrl || galleryUrls.includes(url)) {
          window.alert("That picture URL is already on this row.");
          return;
        }
        if (!coverUrl && !galleryUrls.length) {
          setFieldValue(coverField, url);
          return;
        }
        galleryUrls.push(url);
        setFieldValue(galleryField, galleryUrls.join("\n"));
      }

      function deletePhoto(kind, indexValue) {
        if (kind === "cover") {
          setFieldValue(coverField, "");
          return;
        }
        const galleryUrls = parseGalleryValue(galleryField.value);
        const index = Number(indexValue);
        if (Number.isNaN(index) || index < 0 || index >= galleryUrls.length) {
          return;
        }
        galleryUrls.splice(index, 1);
        setFieldValue(galleryField, galleryUrls.join("\n"));
      }

      if (addLogoButton) {
        addLogoButton.addEventListener("click", addLogo);
      }
      if (addPhotoButton) {
        addPhotoButton.addEventListener("click", addPhoto);
      }

      editor.addEventListener("click", (event) => {
        const deleteLogoButton = event.target.closest("[data-delete-logo]");
        if (deleteLogoButton) {
          setFieldValue(logoField, "");
          return;
        }
        const deletePhotoButton = event.target.closest("[data-delete-photo-kind]");
        if (!deletePhotoButton) {
          return;
        }
        deletePhoto(
          deletePhotoButton.getAttribute("data-delete-photo-kind") || "",
          deletePhotoButton.getAttribute("data-delete-photo-index")
        );
      });

      [logoField, coverField, galleryField, clubNameField].forEach((field) => {
        if (!field) {
          return;
        }
        field.addEventListener("input", render);
        field.addEventListener("row-sync", render);
      });

      render();
    });
  }

  initDiscoveryPolling();
  initSelectionCheckboxes();
  initBulkActions();
  initAutosaveForms();
  initInlineRowEditors();
  bindImageDimensions(document);
  initMediaEditors();
})();
