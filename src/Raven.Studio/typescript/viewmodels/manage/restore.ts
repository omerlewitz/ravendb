import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import accessHelper = require("viewmodels/shell/accessHelper");
import database = require("models/resources/database");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");
import monitorRestoreCommand = require("commands/maintenance/monitorRestoreCommand");
import startDbRestoreCommand = require("commands/maintenance/startRestoreCommand");
import resourcesManager = require("common/shell/resourcesManager");
import eventsCollector = require("common/eventsCollector");

class resourceRestore {
    defrag = ko.observable<boolean>(false);
    backupLocation = ko.observable<string>("");
    resourceLocation = ko.observable<string>();
    indexesLocation = ko.observable<string>();
    journalsLocation = ko.observable<string>();
    resourceName = ko.observable<string>();
    nameCustomValidityError: KnockoutComputed<string>;

    restoreStatusMessages = ko.observableArray<string>();
    
    keepDown = ko.observable<boolean>(false);

    constructor(private parent: restore, private type: string, private resources: KnockoutObservableArray<database>) {
        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage = "";
            var newResourceName = this.resourceName();
            var foundDb = resources().find((rs: database) => newResourceName === rs.name);

            if (!!foundDb && newResourceName.length > 0) {
                errorMessage = (this.type === database.type ? "Database" : "File System") + " name already exist!";
            }

            return errorMessage;
        });
    }

    toggleKeepDown() {
        eventsCollector.default.reportEvent("restore", "keep-down", this.type.toString());

        this.keepDown.toggle();
        this.forceKeepDown();
    }

    forceKeepDown() {
        if (this.keepDown()) {
            var body = document.getElementsByTagName("body")[0];
            body.scrollTop = body.scrollHeight;
        }
    }

    updateRestoreStatus(newRestoreStatus: restoreStatusDto) {
        this.restoreStatusMessages(newRestoreStatus.Messages);
        this.forceKeepDown();
        this.parent.isBusy(newRestoreStatus.State === "Running");
    }
}

class restore extends viewModelBase {
    private resourceManager = resourcesManager.default;

    private dbRestoreOptions: resourceRestore = new resourceRestore(this, database.type, this.resourceManager.databases);

    disableReplicationDestinations = ko.observable<boolean>(false);
    generateNewDatabaseId = ko.observable<boolean>(false);
    isForbidden = ko.observable<boolean>();
    isBusy = ko.observable<boolean>();
    anotherRestoreInProgres = ko.observable<boolean>(false);

    canActivate(args: any): any {
        var deferred = $.Deferred();

        this.isForbidden(accessHelper.isGlobalAdmin() === false);
        if (this.isForbidden() === false) {
            this.isBusy(true);
            var self = this;

            new getDocumentWithMetadataCommand("Raven/Restore/InProgress", null, true).execute()
                .fail(() => deferred.resolve({ redirect: appUrl.forSettings(null) }))
                .done((result) => {
                    if (result) {
                        // looks like another restore is in progress
                        this.anotherRestoreInProgres(true);
                        new monitorRestoreCommand($.Deferred(), s => {
                            self.dbRestoreOptions.updateRestoreStatus.bind(self.dbRestoreOptions)(s);
                        })
                            .execute()
                            .always(() => {
                                $("#databaseRawLogsContainer").resize();
                                this.anotherRestoreInProgres(false);
                            });

                    } else {
                        this.isBusy(false);
                    }
                    deferred.resolve({ can: true });
                });
        } else {
            deferred.resolve({ can: true });
        }

        return deferred;
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("FT7RV6");
    }

    startDbRestore() {
        eventsCollector.default.reportEvent("database", "restore");

        this.isBusy(true);
        var self = this;

        var restoreDatabaseDto: databaseRestoreRequestDto = {
            BackupLocation: this.dbRestoreOptions.backupLocation(),
            DatabaseLocation: this.dbRestoreOptions.resourceLocation(),
            DatabaseName: this.dbRestoreOptions.resourceName(),
            DisableReplicationDestinations: this.disableReplicationDestinations(),
            GenerateNewDatabaseId: this.generateNewDatabaseId(),
            IndexesLocation: this.dbRestoreOptions.indexesLocation(),
            JournalsLocation: this.dbRestoreOptions.journalsLocation()
        };

        new startDbRestoreCommand(this.dbRestoreOptions.defrag(), restoreDatabaseDto, self.dbRestoreOptions.updateRestoreStatus.bind(self.dbRestoreOptions))
            .execute();
    }

}

export = restore;  
